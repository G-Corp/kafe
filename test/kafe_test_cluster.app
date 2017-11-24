{application, kafe_test_cluster, [
    {applications, [lager, erlexec]}
  , {modules, [kafe_test_cluster, kafe_test_cluster_broker]}
  , {mod, {kafe_test_cluster, []}}
 ]}.
