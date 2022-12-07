import os
import yaml


class MLRunSpark:
    @staticmethod
    def submit_spark(spark_yaml_template, app_name, main_app_file, spark_conf, arguments, node_label_key='igz_node_label', **kwargs):

        spark_yaml_template['spec']['sparkConf'].update(spark_conf)
        spark_final_conf = spark_yaml_template['spec']['sparkConf'].copy()

        os.environ["MLRUN_DBPATH"] = spark_final_conf.get("mlrun.dbpath")
        os.environ["V3IO_USERNAME"] = spark_final_conf.get("mlrun.user")
        os.environ["V3IO_API"] = spark_final_conf.get("mlrun.v3io.api")
        os.environ["V3IO_ACCESS_KEY"] = spark_final_conf.get("mlrun.v3io.access.key")
        spark_final_conf.pop("mlrun.v3io.access.key")

        from mlrun.run import new_function
        from mlrun import config
        config.config.spark_operator_version = 'spark-3'

        app_image = spark_yaml_template['spec']['image']

        sj = new_function(kind='spark', command=main_app_file, name=app_name, image=app_image, args=arguments)

        # updating driver and executor memory & cores
        driver_memory = spark_final_conf.get('spark.driver.memory')
        driver_cores = spark_final_conf.get('spark.driver.cores')
        executor_memory = spark_final_conf.get('spark.executor.memory')
        executor_cores = spark_final_conf.get('spark.executor.cores')

        driver_kube_req_cores = spark_final_conf.get('spark.kubernetes.driver.request.cores')
        if driver_kube_req_cores is None:
            driver_kube_req_cores = driver_cores

        exec_kube_req_cores = spark_final_conf.get('spark.kubernetes.executor.request.cores')
        if exec_kube_req_cores is None:
            exec_kube_req_cores = executor_cores

        if driver_memory:
            sj.with_driver_requests(cpu=driver_kube_req_cores, mem=driver_memory)
        if executor_memory:
            sj.with_executor_requests(cpu=exec_kube_req_cores, mem=executor_memory)
        
        driver_cores_int = int(driver_cores) if driver_cores is not None else None
        executor_cores_int = int(executor_cores) if executor_cores is not None else None
        sj.with_cores(driver_cores=driver_cores_int, executor_cores=executor_cores_int)

        # converting yarn config to kubernetes config
        max_attempts = spark_final_conf.get('spark.yarn.maxAppAttempts')
        if max_attempts:
            retry = int(max_attempts) - 1
            sj.with_restart_policy(retries=retry)

        exec_instance = spark_final_conf.get('spark.executor.instances')
        if exec_instance:
            sj.spec.replicas = exec_instance

        if driver_cores:
            sj.with_driver_limits(cpu=driver_cores)
        if executor_cores:
            sj.with_executor_limits(cpu=executor_cores)
        
        # Dynamic Allocation
        dynamic_enabled = spark_final_conf.get('spark.dynamicAllocation.enabled')
        max_executors = spark_final_conf.get('spark.dynamicAllocation.maxExecutors')
        
        if dynamic_enabled:
            sj.with_dynamic_allocation(min_executors=1, initial_executors=1)
            if max_executors:
                sj.with_dynamic_allocation(min_executors=1, max_executors=int(max_executors), initial_executors=1)

        driver_node_label =  kwargs.get('driver_node_label')
        if driver_node_label:
            sj.with_driver_node_selection(node_selector={node_label_key: driver_node_label})
        
        executor_node_label =  kwargs.get('executor_node_label')
        if executor_node_label:
            sj.with_executor_node_selection(node_selector={node_label_key: executor_node_label})
        
        sj.spec.spark_conf = spark_final_conf

        image_pull_policy = spark_yaml_template['spec']['imagePullPolicy']
        if image_pull_policy:
            sj.spec.image_pull_policy=image_pull_policy
        image_pull_secrets = spark_yaml_template['spec']['imagePullSecrets'][0]['name']
        if image_pull_secrets:
            sj.spec.image_image_pull_secret=image_pull_secrets

        sj.with_igz_spark()
        sj.run(artifact_path='local:///tmp/')
