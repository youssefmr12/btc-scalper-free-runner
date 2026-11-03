import { HertzFlowSDK } from "@hertzflow/sdk-v2";
const RPCS=[
 "https://bsc-dataseed.bnbchain.org",
 "https://bsc-rpc.publicnode.com"
];
const BNB_USD=699.92946088;
const PREC=10n**30n;
async function main(){
 let sdk, rpc, last;
 for(const u of RPCS){
  try{
   sdk=new HertzFlowSDK({chainId:56,rpcUrl:u,oracleUrl:""});
   await sdk.publicClient.getBlockNumber();
   rpc=u;break;
  }catch(e){last=e;sdk?.destroy?.();}
 }
 if(!sdk) throw last||new Error("no rpc");
 const gasLimits=await sdk.utils.getGasLimits();
 const gasPrice=await sdk.utils.getGasPrice();
 const oracleCount=3n;
 function adjusted(exec){
   return gasLimits.estimatedGasFeeBaseAmount
    + gasLimits.estimatedGasFeePerOraclePrice*oracleCount
    + exec*gasLimits.estimatedFeeMultiplierFactor/PREC;
 }
 const inc=adjusted(gasLimits.increaseOrder);
 const dec=adjusted(gasLimits.decreaseOrder);
 function econ(g){
   const wei=g*gasPrice;
   const bnb=Number(wei)/1e18;
   return {gas_limit:g.toString(),fee_wei:wei.toString(),fee_bnb:bnb,fee_usd:bnb*BNB_USD};
 }
 const out={
  rpc,
  block:(await sdk.publicClient.getBlockNumber()).toString(),
  bnb_usd:BNB_USD,
  gas_price_wei:gasPrice.toString(),
  gas_price_gwei:Number(gasPrice)/1e9,
  gas_limits:Object.fromEntries(Object.entries(gasLimits).map(([k,v])=>[k,v.toString()])),
  increase_execution_fee:econ(inc),
  decrease_execution_fee:econ(dec),
  round_trip_execution_fee_usd:econ(inc).fee_usd+econ(dec).fee_usd
 };
 console.log("HERTZ_EXEC_FEE "+JSON.stringify(out));
 await sdk.destroy?.();
}
main().catch(e=>{console.error(e);process.exit(1)});
