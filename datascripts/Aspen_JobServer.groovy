package commons.scripts

import java.awt.JobAttributes;
import java.util.Map;
import java.util.Map.Entry

import org.codehaus.jackson.map.ObjectMapper;

import com.typesafe.config.ConfigFactory;

import ai.vital.aspen.config.AspenConfig;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.jobserver.SparkRDDJob;
import ai.vital.aspen.jobserver.client.JobServerClient;
import ai.vital.prime.groovy.v2.VitalPrimeGroovyScriptV2;
import ai.vital.prime.groovy.v2.VitalPrimeScriptInterfaceV2;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList;


/**
 * Datascript that wraps job server api
 * @author Derek
 *
 */
class Aspen_JobServer implements VitalPrimeGroovyScriptV2 {

	static JobServerClient client
	
	static ObjectMapper mapper = new ObjectMapper()
	
	static JobServerClient getClient() {
		
		if(client == null) {
			
			synchronized (Aspen_JobServer.class) {
				
				if(client == null) {
				
					String jobServerURL = AspenConfig.get().getJobServerURL()
					
					if(!jobServerURL) {
						throw new RuntimeException("No jobServerURL set in aspen config - datascript disabled")
					}
					
					client = new JobServerClient(jobServerURL)
						
				}
				
			}
			
		}
		
		return client
		
	}
	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterfaceV2 scriptInterface,
			Map<String, Object> parameters) {

		ResultList rl = new ResultList()
		
		try {
			
			JobServerClient client = getClient()
			
			String action = parameters.get('action')
			
			if(!action) throw new Exception("No 'action' parameter")
			
			if('listrdds'.equals(action)) {
				
				String appName = parameters.get('appName')
				String context = parameters.get('context')
				
				if(!appName) throw new Exception("No 'appName' parameter")
				if(!context) throw new Exception("No 'context' parameter")
				
				String paramsString = "" +
				"action: list\n";
				
				LinkedHashMap<String, Object> results = client.jobs_post(appName, SparkRDDJob.class.getCanonicalName(), context, true, paramsString);
				
				rl.setStatus(VitalStatus.withOKMessage( mapper.defaultPrettyPrintingWriter().writeValueAsString(results) ) );
				
			} else if('removerdd'.equals(action)){
			
				String appName = parameters.get('appName')
				String context = parameters.get('context')
				String rddName = parameters.get('rddName');
				
				if(!appName) throw new Exception("No 'appName' parameter")
				if(!context) throw new Exception("No 'context' parameter")
				if(!rddName) throw new Exception("No 'rddName' parameter")
			
				String paramsString = "" +
					"action: remove\n" +
					"rdd-name: \"" + rddName + "\""
				;
			
				LinkedHashMap<String, Object> results = client.jobs_post(appName, SparkRDDJob.class.getCanonicalName(), context, true, paramsString);
				
				rl.setStatus(VitalStatus.withOKMessage( mapper.defaultPrettyPrintingWriter().writeValueAsString(results) ) );
				
			} else if('listapps'.equals(action)) {
			
				LinkedHashMap<String, Object> jars_get = client.jars_get();
			
				String o = "Apps (jars) count: " + jars_get.size();
			
				int i = 1;
			
				for(Entry<String, Object> jar : jars_get.entrySet() ) {
				
					o += ("\n" + i + ".   " + jar.getKey() + "   " + jar.getValue() );
				
					i++
				}
				
				rl.setStatus(VitalStatus.withOKMessage( o ));
					
			} else if('listcontexts'.equals(action)) {
			
				List<String> ctxs = client.contexts_get();
			
				String o = ("Contexts count: " + ctxs.size());
			
				for(int i = 1; i <= ctxs.size(); i++) {
				
					o += ( "\n" + i + ".   " + ctxs.get(i-1));
				
				}
			
				rl.setStatus(VitalStatus.withOKMessage( o ));
			
			} else if('postcontext'.equals(action)) {
			
				String context = parameters.get('context')
				String contextParamsURLEncoded = parameters.get('contextParamsURLEncoded')
				
				if(!context) throw new Exception("No 'context' parameter")
			
				
				String o = client.contexts_post(context, contextParamsURLEncoded);
				rl.setStatus(VitalStatus.withOKMessage( o ));
			
			} else if('deletecontext'.equals(action)) {
			
				String context = parameters.get('context')
			
				if(!context) throw new Exception("No 'context' parameter")
				
				rl.setStatus( VitalStatus.withOKMessage( client.contexts_delete(context) ) );
				
			} else if('listjobs'.equals(action)) {
			
				List<LinkedHashMap<String, Object>> r = client.jobs_get();
				
				String o = ("All jobs count: " + r.size());
			
				int c = 1;
				for(LinkedHashMap<String, Object> job : r ) {
				
					o += ("\n\n#" + c);
					o += ("\njobId: " + job.get("jobId"));
					o += ("\nstatus: " + job.get("status"));
					o += ("\nclassPath: " + job.get("classPath"));
					o += ("\ncontext: " + job.get("context"));
					o += ("\nstartTime: " + job.get("startTime"));
					o += ("\nduration: " + job.get("duration"));
					
					c++;
					
				}
				
				rl.setStatus(VitalStatus.withOKMessage( o ));
				
			} else if('getjob'.equals(action)) {
			
				String jobId = parameters.get('jobId')
				if(!jobId) throw new Exception("No 'jobId' parameter")
				
				LinkedHashMap<String, Object> details = client.jobs_get_details(jobId);
				
				rl.setStatus(VitalStatus.withOKMessage( mapper.defaultPrettyPrintingWriter().writeValueAsString(details) ) );
			
			} else if('deletejob'.equals(action)) {
			
				String jobId = parameters.get('jobId')
				if(!jobId) throw new Exception("No 'jobId' parameter")
			
				LinkedHashMap<String, Object> status = client.jobs_delete(jobId);
				
				rl.setStatus(VitalStatus.withOKMessage( mapper.defaultPrettyPrintingWriter().writeValueAsString(status) ) );

			} else if('postjob') {
			
				String appName = parameters.get('appName')
				if(!appName) throw new Exception("No 'appName' parameter")
			
				String classPath = parameters.get('classPath')
				if(!classPath) throw new Exception("No 'classPath' parameter")
			
				//optional
				String context = parameters.get('context')
			
				Boolean sync = parameters.get('sync')
				if(sync == null) sync = false
			
			
				String paramsString = parameters.get('paramsString')
				if(!paramsString) throw new Exception("No 'paramsString' parameter")
				
				try {
					ConfigFactory.parseString(paramsString);
				} catch(Exception e) {
					throw new Exception("Job parameters are not valid: " + e.getLocalizedMessage());
				}
			
			
				LinkedHashMap<String, Object> status = client.jobs_post(appName, classPath, context, sync, paramsString);
				
				rl.setStatus(VitalStatus.withOKMessage( mapper.defaultPrettyPrintingWriter().writeValueAsString(status) ) );
				
			} else {
				throw new Exception("Unknown action: ${action}")
			}
			
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()))
		}
		
		return rl;
	}

	
	
}
