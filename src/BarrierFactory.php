<?php

namespace DtmClient;

use DtmClient\Constants\DbType;
use DtmClient\Exception\UnsupportedException;

class BarrierFactory
{
    public static function call()
    {
        switch (config('dtm.barrier_db_type', DbType::MySql)) {
            case DbType::MySql:
                return MySqlBarrier::call();
            case DbType::Redis:
                return null;
            default:
                throw new UnsupportedException('barrier db type is unsupported.');
        }
    }
    
    public static function barrierFrom(string $transType, string $gid, string $branchId, string $op)
    {
        switch (config('dtm.barrier_db_type', DbType::MySql)) {
            case DbType::MySql:
                return MySqlBarrier::barrierFrom($transType, $gid, $branchId, $op);
            case DbType::Redis:
                return null;
            default:
                throw new UnsupportedException('barrier db type is unsupported.');
        }
    }
    
}