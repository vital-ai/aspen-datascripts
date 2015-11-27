package commons.scripts

import java.io.Serializable;
import java.util.Map

import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.ModelManager
import ai.vital.prime.groovy.TimeUnit;
import ai.vital.prime.groovy.VitalPrimeGroovyJob;
import ai.vital.prime.groovy.VitalPrimeGroovyScript;
import ai.vital.prime.groovy.VitalPrimeScriptInterface;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.property.URIProperty;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class Aspen_ModelManagerJob implements VitalPrimeGroovyScript, VitalPrimeGroovyJob {

	public final static String modelManagerKey = 'aspen-model-manager'
	
	private final static Logger log = LoggerFactory.getLogger(Aspen_ModelManagerJob.class)
	
	@Override
	public ResultList executeScript(
			VitalPrimeScriptInterface scriptInterface,
			Map<String, Object> parameters) {

		//only single reload at a time!
		synchronized (Aspen_ModelManagerJob) {
			
		ResultList rl = new ResultList()
		
		try {

			ModelManager manager = scriptInterface.getRegistry().get(modelManagerKey);
			
			if(manager == null) {
				log.info("No model manager - nothing to do")
				rl.setStatus(VitalStatus.withOKMessage("No model manager instance found"))
				return rl;
			}
			
			
			List<AspenModel> models = manager.getLoadedModels();
			
			int checked = 0
			int reloaded = 0
			int unchanged = 0
			int errors = 0
			
			StringBuilder errorsMsgs = new StringBuilder()
			
			List<URIProperty> failedURIs = []
			
			
			for(AspenModel model : models) {
				
				FileSystem fs = null
				
				checked ++
				
				try {
					
					Date currentTimestamp = model.getTimestamp()
					
					Path path = new Path(model.getSourceURL());
					
					fs = FileSystem.get(path.toUri(), AspenGroovyConfig.get().getHadoopConfiguration())
					
					FileStatus fileStatus = fs.getFileStatus(path)
					
					long modTime = fileStatus.getModificationTime()
					
					if( currentTimestamp == null || currentTimestamp.getTime() != modTime ) {
						
						log.info("Reloading model ${model.URI}, ${model.getName()} - changed timestamp...")
						manager.loadModel(model.getSourceURL(), true)
						reloaded++
						
						log.info("Model reloaded successfully: ${model.URI}, ${model.getName()}")
						
					} else {
					
						log.info("model unchanged, ${model.URI}, ${model.getName()}")
						unchanged ++
					}
				
				} catch(Exception e) {
				
					log.error("Error when reloading model ${model.URI}, ${model.getName()}: ${e.localizedMessage}")
					errors++
				
					if(errorsMsgs.length() > 0 ) { errorsMsgs.append("\n") }
					errorsMsgs.append(e.getLocalizedMessage())
					failedURIs.add(URIProperty.withString(model.getURI()))
				
				} finally {
					IOUtils.closeQuietly(fs)
				}
				
			}
			
			
			String statusMsg = "Iterated models: ${checked}, unchanged: ${unchanged}, reloaded: ${reloaded}, errors: ${errors}";
			
			if(errorsMsgs.length() > 0) {
				statusMsg += " ${errorsMsgs.toString()}"
			}
			
			VitalStatus status = VitalStatus.withOKMessage(statusMsg)
			status.setErrors(errors)
			status.setSuccesses(reloaded)
			status.setFailedURIs(failedURIs)
			rl.setTotalResults(checked)
			
			rl.setStatus(status)
			
		} catch(Exception e) {
			rl.setStatus(VitalStatus.withError(e.getLocalizedMessage()))
		}
		
		return rl;
		
		}
	}

	@Override
	public boolean startAtPrettyTime() {
		return false;
	}

	@Override
	public int getInterval() {
		return 30;
	}

	@Override
	public TimeUnit getIntervalTimeUnit() {
		return TimeUnit.MINUTE;
	}

	@Override
	public void executeJob(VitalPrimeScriptInterface scriptInterface,
			Map<String, Serializable> jobDataMap) {
			
		log.info("Reloading models job started")
			
		executeScript(scriptInterface, [:])
	}

}
