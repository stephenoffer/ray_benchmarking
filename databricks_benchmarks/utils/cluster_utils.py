from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster
import ray

from ray.runtime_env import RuntimeEnv


def init_cluster(restart=True,
                 max_worker_nodes=8,
                 min_worker_nodes=8,
                 num_cpus_per_node=8,
                 num_gpus_per_node=0,
                 log_path="/dbfs/soffer/ray_logs"):
    """

    """
    if restart is True:
        if ray.is_initialized():
            shutdown_ray_cluster()
            ray.shutdown()

    if not ray.is_initialized():
        setup_ray_cluster(
            max_worker_nodes=max_worker_nodes,
            min_worker_nodes=min_worker_nodes,
            num_cpus_per_node=num_cpus_per_node,
            num_gpus_per_node=num_gpus_per_node,
            collect_log_to_path=log_path
        )
        runtime_env = {"env_vars": {"GLOO_SOCKET_IFNAME": "eth0"}}
        ray.init(ignore_reinit_error=True, runtime_env=RuntimeEnv(env_vars=runtime_env['env_vars']))

    print(ray.cluster_resources())
