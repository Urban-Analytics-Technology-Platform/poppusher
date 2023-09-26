import { error } from '@sveltejs/kit';
import duckdb from 'duckdb'

/** @type {import('./$types').RequestHandler} */
export async function GET({ url }) {
	const metrics  =url.searchParams.get('metrics') ?? 'B01001_E001' ;
  console.log("Metrics ", metrics)

  const db = new duckdb.Database(':memory:'); // or a file name for a persistent DB
  console.time("fetch")
  try{
    const result = await new Promise((resolve,reject) =>{
      db.all(`INSTALL httpfs; LOAD httpfs; Select ${metrics} from read_parquet('https://allofthedata.s3.us-west-2.amazonaws.com/acs/tracts_2019_fiveYear.parquet')`,(err,result)=>{
      console.timeEnd("fetch")
      if(err){
        reject(err)
      }
        resolve(result)
      })
    })
      return new Response(JSON.stringify(result))
  }
  catch(e){
      return new Response(JSON.stringify({error:e.message}))
  }
}
