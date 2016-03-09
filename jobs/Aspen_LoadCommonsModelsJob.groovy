package commons.scripts

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import ai.vital.aspen.groovy.AspenGroovyConfig
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.modelmanager.ModelManager
import ai.vital.prime.conf.VitalPrimeConfigurationException
import ai.vital.prime.files.FilesComponent
import ai.vital.prime.groovy.S3Conf
import ai.vital.prime.groovy.TimeUnit
import ai.vital.prime.groovy.admin.VitalPrimeAdminGroovyJob
import ai.vital.prime.groovy.admin.VitalPrimeAdminGroovyScript
import ai.vital.prime.groovy.admin.VitalPrimeAdminScriptInterface
import ai.vital.vitalservice.query.ResultList
import ai.vital.vitalsigns.model.VitalApp

class Aspen_LoadCommonsModelsJob implements VitalPrimeAdminGroovyScript, VitalPrimeAdminGroovyJob {

	/**  CONFIG  **/
	
	//empty while list would load all aspen models from commons
	static List<String> whitelist = [] 
	
	/**  END OF CONFIG  **/
	
	public final static String modelManagerKey = 'aspen-model-manager'
	
	public final static String FTP_URI = 'FTP'
	
	private final static Logger log = LoggerFactory.getLogger(Aspen_LoadCommonsModelsJob.class)
	
	@Override
	public ResultList executeScript(VitalPrimeAdminScriptInterface scriptInterface, Map<String, Object> parameters) {
		executeJob(scriptInterface, [:])
		return new ResultList();
	}

	@Override
	public boolean startAtPrettyTime() {
		//start immediately
		return false;
	}

	@Override
	public int getInterval() {
		return 1;
	}

	@Override
	public TimeUnit getIntervalTimeUnit() {
		return TimeUnit.HOUR;
	}

	@Override
	public void executeJob(VitalPrimeAdminScriptInterface scriptInterface, Map<String, Serializable> jobDataMap) {
		
		log.info("Executing ${Aspen_LoadCommonsModelsJob.class.canonicalName}, whitelist: ${whitelist}")
		
		ModelManager manager = initModelManager(scriptInterface)
		
		Set<String> currentModels = new HashSet<String>();
		
		for(AspenModel m : manager.getLoadedModels()) {
			
			currentModels.add( m.getSourceURL() )
			
		}
		
		log.info("Currently loaded URLs: {}", currentModels)
		
		int skipped = 0
		int errors = 0;
		int existing = 0;
		int loaded = 0;
		
		for(File f : scriptInterface.listFiles(VitalApp.withId(FilesComponent.COMMONS_APP_ID), FTP_URI)) {
			
			if( ! f.name.endsWith(".jar")) continue
			
			if(whitelist.size() > 0 && ! whitelist.contains(f.name)) {
				log.info("File not in the whitelist: ${f.name}")
				skipped++
				continue
			}
			
			String u = f.toURI().toString()
			
			if(currentModels.contains(u)) {
				log.info("Model already loaded: ${u}")
				existing++
				continue
			}
			
			
			log.info("Loading common model, path: ${u}")
			try {
				manager.loadModel(u, true)
				loaded++
				log.info("Model loaded: " + u)
			} catch(Exception e) {
				log.error("Error when loading from file: " + u + " - " + e.localizedMessage, e)
				errors++
			}
			
			
		}
		
		log.info("${Aspen_LoadCommonsModelsJob.class.canonicalName} status - existing: ${existing}, loaded: ${loaded}, errors: ${errors}, skipped: ${skipped}")
		
		
	}
	
	ModelManager initModelManager(VitalPrimeAdminScriptInterface scriptInterface) {
		
		ModelManager manager = scriptInterface.getRegistry().get(modelManagerKey);
		
		if(manager != null) return manager
		
		synchronized (ModelManager.class) {

			manager = scriptInterface.getRegistry().get(modelManagerKey);
			
			if(manager != null) return manager
			
			AspenGroovyConfig config = AspenGroovyConfig.get()
			
			try {
				
				S3Conf s3Conf = scriptInterface.getS3Config();
				config.setAWSAccessCredentials(s3Conf.awsAccessKeyId, s3Conf.awsSecretAccessKey)
				
			} catch(VitalPrimeConfigurationException e) {
				//ignore
			}
			
			try {
				
				List<String> xmls = scriptInterface.getHadoopConfig()
				config.setHadoopConfigXML(xmls)
				
			} catch(VitalPrimeConfigurationException e) {
				//ignore
			}
			
			manager = new ModelManager()
			
			
			
			scriptInterface.getRegistry().put(modelManagerKey, manager)
			
			return manager
						
		}
		
	}
	

}
