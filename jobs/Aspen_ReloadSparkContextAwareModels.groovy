package commons.scripts

import java.io.Serializable;
import java.util.Map

import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.modelmanager.ModelManager
import ai.vital.aspen.model.AspenCollaborativeFilteringPredictionModel;
import ai.vital.prime.groovy.TimeUnit;
import ai.vital.prime.groovy.VitalPrimeGroovyJob
import ai.vital.prime.groovy.VitalPrimeGroovyScript;
import ai.vital.prime.groovy.VitalPrimeScriptInterface
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.query.ResultList;

class Aspen_ReloadSparkContextAwareModels implements VitalPrimeGroovyJob, VitalPrimeGroovyScript {

	private final static Logger log = LoggerFactory.getLogger(Aspen_ReloadSparkContextAwareModels.class)
	
	public final static String modelManagerKey = 'aspen-model-manager'
	
	@Override
	public boolean startAtPrettyTime() {
		return false;
	}

	@Override
	public int getInterval() {
		return 24;
	}

	@Override
	public TimeUnit getIntervalTimeUnit() {
		return TimeUnit.HOUR;
	}

	@Override
	public void executeJob(VitalPrimeScriptInterface scriptInterface, Map<String, Serializable> jobDataMap) {

		executeScript(scriptInterface, [:])
		
	}

	@Override
	public ResultList executeScript(VitalPrimeScriptInterface scriptInterface, Map<String, Object> parameters) {

		ResultList rl = new ResultList()
		
		ModelManager manager = scriptInterface.getRegistry().get(modelManagerKey);
		
		if(manager == null) {
			log.info("No model manager - nothing to do")
			rl.setStatus(VitalStatus.withOKMessage("No model manager instance found"))
			return rl;
		}
		
		int reloaded = 0
		int errors = 0
		
		List<AspenModel> models = manager.getLoadedModels();
		
		for(AspenModel model : models) {
			
			if(model instanceof AspenCollaborativeFilteringPredictionModel) {
				
				log.info("Reloading model: ${model.URI} - ${model.name} [${model.class.canonicalName}")
				try {
					manager.unloadModelByURI(model.URI)
					manager.loadModel(model.getSourceURL(), true)
					reloaded++
				} catch(Exception e) {
					log.error(e.getLocalizedMessage(), e)
					errors++;
				}
				
			}
			
		}
		
		log.info("Job completed, reloaded ${reloaded} models, errors: ${errors}")
		
		rl.status.successes = reloaded
		rl.status.errors = errors
		
		return rl;
	}

}
