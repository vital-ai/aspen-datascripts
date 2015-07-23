package commons.scripts

import groovy.json.JsonOutput;
import groovy.json.JsonSlurper;

import java.util.Map;

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.log4j.chainsaw.LoggingReceiver.Slurper;

import ai.vital.prime.VitalPrime;
import ai.vital.prime.groovy.v2.VitalPrimeGroovyScriptV2;
import ai.vital.prime.groovy.v2.VitalPrimeScriptInterfaceV2;
import ai.vital.prime.uribucket.UriBucket;
import ai.vital.prime.uribucket.UriBucketComponent;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultElement
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.VITAL_Category

class Aspen_Usage implements VitalPrimeGroovyScriptV2 {

	static String action_getUsage = 'getUsage'
	
	static String action_incrementUsage = 'incrementUsage'
	
	static List<String> actions = [action_getUsage, action_incrementUsage]
	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterfaceV2 scriptInterface,
			Map<String, Object> parameters) {

		ResultList rl = new ResultList()
		
		try {
			
			String action = parameters.get('action')
			
			if(!action) throw new Exception("No 'action' parameter")
			
			String key = parameters.get('key')
			
			if(!key) throw new Exception("No 'key' parameter")
			
			if( ! VitalPrime.get().uriBucketsComponent ) throw new Exception("No uri buckets component")
			
			UriBucketComponent bucket = VitalPrime.get().uriBucketsComponent.getStatsBucket();
			
			if(bucket == null) throw new Exception("No stats bucket");
			
			if(! actions.contains(action)) throw new Exception("Unknown action: ${action}, valid actions: ${actions}")
			
			
			int currentUsage = 0
			
			List<UriBucket> buckets = bucket.getLastNBuckets(true, 24)
			
			GregorianCalendar current = new GregorianCalendar()
			int dayOfMonth = current.get(GregorianCalendar.DAY_OF_MONTH)
			
			for(UriBucket b : buckets) {
				
				GregorianCalendar gc = new GregorianCalendar()
				gc.setTime(b.getBucketStart())
				
				int d = gc.get(GregorianCalendar.DAY_OF_MONTH)
				
				if(d == dayOfMonth) {
					
					Integer v = b.getHistogram().get(key)
					
					if(v == null) v = 0
					
					currentUsage += v
					
				}
				
			}
			
			if(action == action_getUsage) {
				
				rl.setLimit(currentUsage)
				rl.setTotalResults(currentUsage)
				
			} else if(action == action_incrementUsage) {

//				[action: 'incrementUsage', key:'metamind', increment: 1, limit: 1000])
			
				Integer limit = parameters.get('limit')
				if(limit == null) throw new Exception("No 'limit' parameter")
				
				Integer increment = parameters.get('increment')
				if(increment == null) throw new Exception("No 'increment' parameter")
				if(increment <= 0) throw new Exception("'increment' value must be greater than 0")
				
				int newVal  = currentUsage + increment
				
				if(newVal > limit) throw new Exception("Usage quota exceeded: ${limit}")
						
				bucket.putUri(key, null, increment)	
						
				rl.setLimit(newVal)
				rl.setTotalResults(newVal)
				
			} else throw new Exception("Unhandled action: ${action}")
			
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.localizedMessage))
		}
		
		return rl;
	}

}
