import {readable, writable, derived} from "svelte/store"
import {set} from "lodash"
import {arrangeIntoTree} from "../utils/tree"

type Metric ={
  tableID:string,
  uniqueID:string,
  universe:string,
  variableName:string,
  variableExtendedName:string
}

export const selectedMetric= writable<string | null>(null);

export const metrics = readable<{loading:boolean,value:Array<Metric>| null}>({loading:true,value:null},(set)=>{
  fetch('https://allofthedata.s3.us-west-2.amazonaws.com/acs/tracts_2019_fiveYear_variables.json').then((r)=>
    r.json()
  ).then((metrics)=>{
     set({value:metrics, loading:false})
  }).catch(e=>{
    console.log("Error is ",e)
  })
})


export const metricTree = derived(
  metrics,
  ($metrics)=>{
    if($metrics.loading){
      return $metrics
    }    
    else{
      let vals = $metrics.value
      let paths = $metrics.value.map(v=>({path: [v.universe, ...v.variableExtendedName.split("|")], id: v.uniqueID}))
      console.log("paths ",paths)
      let tree = arrangeIntoTree(paths)

      return {value:tree,loading:false}
    }
  }
)

function reformatId(id:string){
  const parts = id.split("_")
  return parts[0] + "_E"+parts[1]
}

function createMetricValues (){
  const metricStore = writable({loading:true,value:null})

  const loadIds = (ids)=>{
    metricStore.set({loading:true, value:null})
    if(ids && ids.length>0){
      const censusIds = ["GEO_ID", ...ids.map(reformatId)]
      fetch(`/api/census?metrics=${censusIds.join(",")}`).then((r)=>r.json()).then((data)=>{
        console.log("Got result ", data)
        metricStore.set({loading:false, value:data})
      })
    }
  }
  return {...metricStore, setIds:loadIds}

}

function createSelectedMetrics (){
  let metrics = writable<Array<string>>([])

  const toggleMetric =(metricId:string)=>{
    metrics.update( (previous)=>{
      if(previous.includes(metricId)){
        return previous.filter(p=> p.id !==metricId )
      }
      else{
        return [...previous, metricId]
      }
    })
  }

  return {...metrics, toggleMetric}
}

export const metricValues = createMetricValues()
export const selectedMetrics = createSelectedMetrics()
