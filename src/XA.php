<?php

declare(strict_types=1);
/**
 * This file is part of DTM-PHP.
 *
 * @license  https://github.com/dtm-php/dtm-client/blob/master/LICENSE
 */
namespace DtmClient;

use DtmClient\Api\ApiInterface;
use DtmClient\Api\RequestBranch;
use DtmClient\Constants\Branch;
use DtmClient\Constants\Operation;
use DtmClient\Constants\TransType;
use DtmClient\Exception\XaTransactionException;

class XA extends AbstractTransaction
{
    protected Barrier $barrier;

    protected BranchIdGeneratorInterface $branchIdGenerator;

    protected Dtmimp $dtmimp;

    public function __construct(ApiInterface $api, Barrier $barrier, BranchIdGeneratorInterface $branchIdGenerator, Dtmimp $dtmimp)
    {
        $this->api = $api;
        $this->barrier = $barrier;
        $this->branchIdGenerator = $branchIdGenerator;
        $this->dtmimp = $dtmimp;
    }

    /**
     * start a xa local transaction.
     * @param mixed $callback
     * @throws XaTransactionException
     */
    public function localTransaction(array $query, $callback)
    {
        $phase2Url = $this->xaQueryToTransContext($query);
        if (TransContext::getOp() == Branch::BranchCommit || TransContext::getOp() == Branch::BranchRollback) {
            echo '[' . TransContext::getOp() . ']' . TransContext::getGid() . '-' . TransContext::getBranchId() . PHP_EOL;
            $this->dtmimp->xaHandlePhase2(TransContext::getGid(), TransContext::getBranchId(), TransContext::getOp());
            return;
        }

        $this->dtmimp->xaHandleLocalTrans(function () use ($phase2Url, $callback) {
            $callback();
            $body = [
                'url' => $phase2Url,
                'branch_id' => TransContext::getBranchId(),
                'gid' => TransContext::getGid(),
                'trans_type' => TransType::XA,
            ];
            return $this->api->registerBranch($body);
        });
    }

    public function callBranch(string $url, array $body)
    {
        $subBranch = $this->branchIdGenerator->generateSubBranchId();
        $requestBranch = new RequestBranch();
        $requestBranch->body = $body;
        $requestBranch->url = $url;
        $requestBranch->phase2Url = $url;
        $requestBranch->op = Operation::ACTION;
        $requestBranch->method = 'POST';
        $requestBranch->branchId = $subBranch;
        $requestBranch->branchHeaders = TransContext::$branchHeaders;
        return $this->api->transRequestBranch($requestBranch);
    }

    /**
     * start a xa global transaction.
     * @param $callback
     * @throws \Throwable
     */
    public function globalTransaction(string $gid, $callback)
    {
        $this->dtmimp->newTransBase($gid, TransType::XA, '');
        $this->api->prepare(TransContext::toArray());
        try {
            $callback();
            $this->api->submit(TransContext::toArray());
        } catch (\Throwable $throwable) {
            $this->api->abort(TransContext::toArray());
            throw $throwable;
        }
    }

    protected function xaQueryToTransContext(array $params)
    {
        $branchId = $params['branch_id'] ?? '';
        $dtm = $params['dtm'] ?? '';
        $gid = $params['gid'] ?? '';
        $op = $params['op'] ?? '';
        $phase2Url = $params['phase2_url'] ?? '';
        $transType = $params['trans_type'] ?? '';
        $this->barrier->barrierFrom($transType, $gid, $branchId, $op);
        TransContext::setDtm($dtm);
        return $phase2Url;
    }
}
