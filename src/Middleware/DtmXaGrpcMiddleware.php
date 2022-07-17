<?php

declare(strict_types=1);
/**
 * This file is part of DTM-PHP.
 *
 * @license  https://github.com/dtm-php/dtm-client/blob/master/LICENSE
 */
namespace DtmClient\Middleware;

use DtmClient\Barrier;
use DtmClient\TransContext;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

class DtmXaGrpcMiddleware implements MiddlewareInterface
{
    protected Barrier $barrier;

    public function __construct(Barrier $barrier)
    {
        $this->barrier = $barrier;
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
//        var_dump($request->get());
        $headers = $request->getHeaders();
        $this->initTransContext($headers);
        return $handler->handle($request);
    }

    protected function initTransContext(array $params): void
    {
        $dtm = $headers['dtm-dtm'][0] ?? null;
        $gid = $params['dtm-gid'][0] ?? null;
        $transType = $params['dtm-trans_type'][0] ?? null;
        $branchId = $params['dtm-branch_id'][0] ?? null;
        $op = $params['dtm-op'][0] ?? null;
        $phase2Url = $params['dtm-phase2_url'][0] ?? null;
        if ($transType && $gid && $branchId && $op) {
            $this->barrier->barrierFrom($transType, $gid, $branchId, $op);
        }
        $dtm && TransContext::setDtm((string) $dtm);
        $phase2Url && TransContext::setPhase2URL((string) $phase2Url);
    }
}
