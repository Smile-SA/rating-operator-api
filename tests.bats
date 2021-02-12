#!/usr/bin/env bats

START=start=1900-01-01%2000:00:00.0Z
END=end=2999-12-31%2023:59:59.0Z

HTTP () {
     time http --check-status "$@"
}

HTTP_johndoe () {
     time http --check-status "$@" X-Auth-UserId:johndoe
}

@test "/alive" {
    HTTP ":5042/alive"
}

@test "/namespaces" {
    HTTP_johndoe ":5042/namespaces"
}

@test "/namespaces/rating" {
    HTTP_johndoe ":5042/namespaces/rating?$START&$END"
}

@test "/tenants" {
    HTTP ":5042/tenants"
}

@test "/metrics" {
    HTTP_johndoe ":5042/metrics"
}

@test "/pods" {
    HTTP_johndoe ":5042/pods"
}

@test "/pods/<pod>/lifetime" {
    pod=$(kubectl get pods -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/pods/${pod}/lifetime"
}

@test "/pods/rating" {
    HTTP_johndoe ":5042/pods/rating"
}

@test "/pods/total_rating" {
    HTTP_johndoe ":5042/pods/total_rating?$START&$END"
}

@test "/nodes" {
    HTTP_johndoe ":5042/nodes"
}

@test "/nodes/rating" {
    HTTP_johndoe ":5042/nodes/rating"
}

@test "/nodes/total_rating" {
    HTTP_johndoe ":5042/nodes/total_rating"
}

@test "/nodes/<node>/total_rating" {
    node=$(kubectl get nodes -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/total_rating"
}

@test "/namespaces/total_rating" {
    HTTP_johndoe ":5042/namespaces/total_rating"
}

@test "/namespaces/<namespace>/tenant" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/tenant"
}

@test "/namespaces/<namespace>/total_rating" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/total_rating"
}

@test "/namespaces/<namespace>/rating" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/rating"
}

@test "/nodes/<node>/rating" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/rating"
}

@test "/nodes/<node>/namespaces" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/namespaces"
}

@test "/nodes/<node>/pods" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/pods"
}

@test "/nodes/<node>/namespaces/rating" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/namespaces/rating"
}

@test "/nodes/<node>/namespaces/<namespace>/rating" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/namespaces/${namespace}/rating"
}

@test "/nodes/<node>/namespaces/<namespace>/total_rating" {
    node=$(kubectl get node -o name | head -1 | cut -d / -f2)
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/nodes/${node}/namespaces/${namespace}/total_rating"
}

@test "/pods/<pod>/rating" {
    pod=$(kubectl get pods -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/pods/${pod}/rating"
}

@test "/pods/<pod>/total_rating" {
    pod=$(kubectl get pods -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/pods/${pod}/total_rating"
}

@test "/pods/<pod>/namespace" {
    pod=$(kubectl get pods -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/pods/${pod}/namespace"
}

@test "/pods/<pod>/node" {
    pod=$(kubectl get pods -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/pods/${pod}/node"
}

@test "/namespaces/<namespace>/pods" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/pods"
}

@test "/namespaces/<namespace>/nodes" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/nodes"
}

@test "/namespaces/<namespace>/nodes/pods" {
    namespace=$(kubectl get ns -o name | head -1 | cut -d / -f2)
    HTTP_johndoe ":5042/namespaces/${namespace}/nodes/pods"
}

@test "/metrics/<metric>/rating" {
    HTTP_johndoe ":5042/metrics/usage_cpu/rating"
}

@test "/metrics/<metric>/total_rating" {
    HTTP_johndoe ":5042/metrics/usage_cpu/total_rating"
}

@test "/reports/<report>/metric" {
    HTTP_johndoe ":5042/reports/pod-cpu-usage-hourly/metric"
}

@test "/metrics/<metric>/report" {
    HTTP_johndoe ":5042/metrics/usage_cpu/report"
}

@test "/metrics/<metric>/last_rated" {
    HTTP_johndoe ":5042/metrics/usage_cpu/last_rated"
}

@test "/metrics/last_rated" {
    HTTP_johndoe ":5042/metrics/last_rated"
}

@test "/rating/configs/list" {
    HTTP_johndoe ":5042/rating/configs/list"
}

@test "/rating/configs/<timestamp>" {
    HTTP_johndoe ":5042/rating/configs/0"
}

#@test "/rating/configs" {
#    HTTP_johndoe ":5042/rating/configs"
#}
