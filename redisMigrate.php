<?php
/**
 * Created by PhpStorm.
 * User: sun
 * Date: 2019/7/26
 * Time: 23:00
 */

/** - string: Redis::REDIS_STRING
* - set:   Redis::REDIS_SET
* - list:  Redis::REDIS_LIST
* - zset:  Redis::REDIS_ZSET
* - hash:  Redis::REDIS_HASH
* - other: Redis::REDIS_NOT_FOUND*/

/**
 * 基于多进程的redis数据迁移脚本
 * Class RedisMigrate
 */
  $sucessCnt = 0;
  $totalCnt = 0;
class RedisMigrate
{
    static  private $processNum = 8;
    static  private $listStep = 2;
    static  private $scanCnt = 3;
    //static  private $expireTime = 3600*24*7;
    static  private $expireTime = 60;

    private static $redisSrc;
    private static $redisDst;
    //过滤掉一些不需要迁移的key
    private static $whiteKey=[];
    public function __construct()
    {
        self::$redisSrc = new Redis();
        self::$redisSrc->connect('127.0.0.1',6379);
        self::$redisSrc->setOption(Redis::OPT_SCAN,Redis::SCAN_RETRY);
        //self::$redisSrc->ip='127.0.0.1';
        //self::$redisSrc->port=6379;
        self::$redisSrc->select(0);
        //若scan扫描的结果非空，则继续扫描，scan返回为空，则说明已经扫描完毕，否则非空

        self::$redisDst = new Redis();
        self::$redisDst->connect('127.0.0.1',6379);
        self::$redisDst->select(2);
    }
    static function copyString($key)
    {
        try{
            $strVal = self::$redisSrc->get($key);
            if(empty($strVal)){
                return false;
            }
            $res = self::$redisDst->set($key,$strVal);
            return $res;
        }catch (\Exception $e){
            self::Log($key);
            return false;
        }

    }

    /**
     * 使用zscan读取有序序列中的数据，避免使用zrange
     * @param $key
     * @return bool
     */
    static public function copyZSet( $key)
    {
        $it = NULL;
        try{
            while($arrMatches =  self::$redisSrc->zScan($key, $it)) {
                self::$redisDst->multi(); //借助管道实现，提升效率
                foreach($arrMatches as $mem => $score) {
                    self::$redisDst->zAdd($key, $score, $mem);
                }
                self::$redisDst->exec();
            }
            return true;
        }catch (\Exception $e){
            self::Log($key);
            return false;
        }

    }

    /**
     * 使用hscan读取hash的k-v对，避免使用hgetall
     * @param $key
     * @return bool
     */
    static function copyHash($key){
        $itor = NULL;
        try{
            while($arrMatches = self::$redisSrc->hScan($key, $itor)) {
                self::$redisDst->hmset($key, $arrMatches);
            }
        }catch (\Exception $e){
            self::Log($key);
            return false;
        }

        return true;
    }

    /**
     * 使用迭代的方式读取set的成员，避免使用smembers
     * @param $key
     * @return bool
     */
    static function copySet($key)
    {
        $itor = NULL;
        try{
            while($arrMatches = self::$redisSrc->Sscan($key, $itor)) {
                //var_dump($arrMatches);
                self::$redisDst->sAdd($key, $arrMatches);
            }
            return true;
            //throw new \Exception('set cuowu');
        }catch (\Exception $e){

            self::Log($key);
            return false;
        }


    }
    /**
     * 分段copy队列，避免一次性的读取所有的成员
     * @param $key
     * @return bool
     */
    static function copyList($key)
    {
        try{
            $start =0;
            //分段移动list数据
            while($listarr = self::$redisSrc->lRange($key,$start,$start+self::$listStep-1)){
                //var_dump($listarr);
                foreach ($listarr as $listtmp){
                    self::$redisDst->lPush($key,$listtmp);
                }
                $start = $start+self::$listStep;
            }
            return true;
        }catch (\Exception $e){
            self::Log($key);
            return false;
        }

    }
    /**
     * @param $msg
     */
    static function Log($key)
    {
        $msg= date('Y-m-d H:i:s')." redisSrc:".self::$redisSrc->ip." port:".self::$redisSrc->port." key:".$key."\n";
        file_put_contents('/tmp/rediscopyErr.log',$msg,8);
    }

    //扫描库中的redis
    public function scankey($pid,$processindex){
        $itor=null;
        while($keysArr  = self::$redisSrc->scan($itor,'*',self::$scanCnt)){
            foreach ($keysArr as $key){
                //echo $key.PHP_EOL;
                //过滤掉一些不需要迁移的key
                if(in_array($key,self::$whiteKey)){
                    continue;
                }
              $this->taskDispatch($pid,$processindex,$key);
                //$this->Migrate($key);
            }
        }
    }

    /**
     * redis数据建分发
     * @param $pidindex
     * @param $key
     */
    public function taskDispatch($pid,$pidindex,$key){
        if((abs(crc32($key))% self::$processNum) == $pidindex){
            $this->Migrate($pid,$key);
            //对原有的key设置过期时间
            //$this->setExpire($key);
        }
    }

    /**
     * 对原有的数据库的数据设置过期时间
     * @param $key
     */
    public function setExpire($key)
    {
        self::$redisSrc->expire($key,self::$expireTime);
    }
    /**
     * redis数据迁移函数
     * @param $key
     */
    public function Migrate($pid,$key)
    {
        $keyType = self::$redisSrc->type("$key");
        switch ($keyType) {
            case Redis::REDIS_SET:
                echo "$pid  $key:set" . PHP_EOL;
                $res = self::copySet($key);
                break;
            case Redis::REDIS_STRING:
                echo "$pid  $key:string" . PHP_EOL;
                $res = self::copyString($key);
                break;//少了你啊，阿门
            case Redis::REDIS_LIST:
                echo "$pid  $key:list" . PHP_EOL;
                $res = self::copyList($key);
                break;
            case Redis::REDIS_ZSET:
                echo "$pid  $key:zset" . PHP_EOL;
                $res = self::copyZSet($key);
                break;
            case Redis::REDIS_HASH:
                echo "$pid  $key:hash" . PHP_EOL;
                $res = self::copyHash($key);
                break;
            default:
                break;
        }
        global  $sucessCnt;
        global  $totalCnt ;
        $sucessCnt += empty($res) ? 0 :1;
        // $failCnt += empty($res) ? 1 : 0;
        $totalCnt +=1;
    }
}


/**
 *
 * 多进程模型的处理方式
 * 不要不上面的类放在一起，否则会出现异常
 */
 function multiProcess()
{
    $status=0;
    //$processArr =[];
    $parenpid = posix_getpid();
    for($i=0;$i<8;$i++){
        $pid = pcntl_fork();
        //$processArr[$pid] =$i;
        //子进程跳出循环继续向下执行，父进程继续fork子进程
        if($pid ==0){
            break;
        }
    }

    //父进程等待回收子进程的资源
    if(posix_getpid()==$parenpid){
        echo "parent :".$parenpid.PHP_EOL;
        $j=0;
        while(1){
            $childpid =pcntl_waitpid(-1,$status,WNOHANG);
            if($childpid>0){
               // echo "child:".$childpid." exit!".PHP_EOL;
                $j++;
            }
            if($j>=8){
                echo "父进程已经回收了所有子进程了".PHP_EOL;
                break;
            }
        }
    }else{//创建的子进程去干一些任务
       // echo "child :".posix_getpid().PHP_EOL;
        // sleep(rand(15,30));
        $ss = new RedisMigrate();
        $ss->scankey(posix_getpid(),$i);
        //统计葛进程redis迁移情况
        global $totalCnt ;
        global $sucessCnt;
        @file_put_contents('/tmp/redisStatic.log',"进程索引：".posix_getpid()." ".date('Y-m-d H:i:s').' totalCnt:'.$totalCnt." sucessCnt:".$sucessCnt.' sucessRate:'.(round($sucessCnt/$totalCnt,3)*100)."\n",8);
        exit;
    }
}
$dd = new RedisMigrate();
//RedisMigrate::copySet('set');
multiProcess();