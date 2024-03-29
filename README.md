# **rating-operator-api**

Used as the communication interface between the rating components, the `rating-operator-api` is also used to expose metrics to users.

There's two ways to access these metrics:

- By watching the results on our Grafana dashboards.
- By querying this API through the exposed endpoints.

In this document we'll explain how to use these endpoints, with examples and expected parameters.

The endpoints are ordered by their resource (namespace, pods, etc..), and the grammar is similar for each category.

There's different kind of queries and right types:

#### Access types

- **Public**, every user of the rating-operator-api will receive the same response.
- **Tenant**, each tenant will receive only data that belongs to him. Tenants that are not logged in will only have access to public data.
- **Admin**, these endpoints can only be used if the tenant is admin and provide the secret key.

#### Differents queries

- **Simple endpoints**

No parameters required, call the endpoint, get a response.

```sh
$ curl http://rating-operator-api.rating:80/namespaces
{
    "results":[{"namespace":"default", "tenant_id":"default"}],
    "total": 1
}
```

- **Endpoints with url parameters**

The endpoints using this method will be labelled `[URL]`.

We'll use `/namespaces/<namespace>/<aggregator>` as an exemple here.

In this query, the time range is handled by the **aggregator**, and the parameters are sent through the url.
```sh
# We use 'default' namespace and the 'daily' aggregator for the example.
$ curl http://rating-operator-api.rating:80/namespaces/default/daily
{
    "results": [...],
    "total": ...
}
```

- **Endpoints with time range (TR)**

The endpoints using this method will be labelled `[TR]`.

For this example, `/namespaces/<namespace>/rating` is a perfect choice.
There's no time range specified in this query, so we need to specify it using url encoded parameters.
The default value for those is from two hours to now.

```sh
$ curl http://rating-operator-api.rating:80/namespaces/default/rating?start=2021-02-05+10%3A22%3A53.604Z&end=2021-02-05+16%3A22%3A53.604Z
{
    "results": [...],
    "total": ...
}
```

- **Endpoint with payload**

The endpoints using this method will be labelled `[PL]`.

These endpoints are the hardest to query, from a user perspective.
They are generally available for resource handling, such as rules or tenant management.
Let's create a RatingRuleModel with cURL as an example:

```sh
$ curl -X POST
       -H "Content-Type: application/json"
       -d '{"name": "test","metric_name": "test","metric": "my_test_promql","timeframe": "3600s"}'
       http://rating-operator-api.rating:80/models/add
RatingRuleModel test created.
```

----

## **Endpoints**

### ***Namespaces***

**GET `/namespaces`** **Public**

Get a list of namespaces visibile by the tenant.
No parameters expected.

**GET `/namespaces/rating`** ***[TR]*** **Tenant**

Get the rating on a time period, grouped by namespaces.

Parameters expected:

- `start`
- `end`

**GET `/namespaces/total_rating`** ***[TR]*** **Tenant**

Get the **sum** of the rating on a time period, grouped by namespaces.

Parameters expected:

- `start`
- `end`

**GET `/namespaces/metrics/rating`** ***[TR]*** **Tenant**

Get the rating on a time period, grouped by namespaces and metrics.

Parameters expected:

- `start`
- `end`

**GET `/namespaces/<namespace>/<aggregator>`** ***[URL]*** **Tenant**

Get the rating for a namespace, according to the aggregator.

Available aggregator are:

- `daily`
- `weekly`
- `monthly`

If `namespace` equal "rating", then the rating is queried for all namespaces.

**GET `/namespaces/<namespace>/rating`** ***[TR] [URL]*** **Tenant**

Get the rating on a time period, for a given namespace.

Parameters expected:

- `start`
- `end`

**GET `/namespaces/<namespace>/total_rating`** ***[TR] [URL]*** **Tenant**

Get the **sum** of the rating on a time period, for a given namespace.

Parameters expected:

- `start`
- `end`

**GET `/namespaces/<namespace>/metrics/<metric>/rating`** ***[TR] [URL]*** **Tenant**

Get the rating on a time period, for givens namespace and metric.

Parameters expected:

- `start`
- `end`


**POST `/namespaces/tenant`** **[PL]** **Admin**

Associate a tenant with a namespace.

Expect a payload with:

- `namespace`
- `tenant_id`
- `token`

### ***METRICS***

**GET `/metrics`** **Public** **Tenant**

Get the list of metrics.

**GET `/metrics/<metric>/todate`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating, from the start of the month to now.

Parameters expected:

- `start`
- `end`

**GET `/metrics/rating`** ***[TR]*** **Tenant**

Get the rating per metric.

Parameters expected:

- `start`
- `end`

**GET `/metrics/<metric>/max`** ***[URL]*** ***[TR]*** **Tenant**

Get the max rating for a given metric.

Parameters expected:

- `start`
- `end`

**GET `/metrics/<metric>/ratio`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating as a ratio per instance, for a given metric.

Parameters expected:

- `start`
- `end`

**GET `/metrics/<metric>/<aggregator>`** ***[URL]*** **Tenant**

Get the rating for a given metric, according to the aggregator.

Available aggregator are:

- `daily`
- `weekly`
- `monthly`

**GET `/metrics/<metric>/rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating for a given metric.

Parameters expected:

- `start`
- `end`

**GET `/metrics/<metric>/total_rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the aggragated rating for a given metric.

Parameters expected:

- `start`
- `end`

### ***PODS***


**GET `/pods`** ***[TR]*** **Tenant**

Get the list of pods.

Parameters expected:

- `start`
- `end`

**GET `/pods/rating`** ***[TR]*** **Tenant**

Get the rating for a given pod.

Parameters expected:

- `start`
- `end`

**GET `/pods/total_rating`** ***[TR]*** **Tenant**

Get pods aggregated rating.

Parameters expected:

- `start`
- `end`

**GET `/pods/<pod>/lifetime`** ***[URL]*** **Tenant**

Get the start and last update time of a pod.

**GET `/pods/<pod>/<aggregator>`** ***[URL]*** **Tenant**

Get the pods rating by time aggregation.

Available aggregator are:

- `daily`
- `weekly`
- `monthly`

**GET `/pods/<pod>/rating`** ***[URL]*** ***[TR]*** **Tenant**

Get pods rating.

Parameters expected:

- `start`
- `end`

**GET `/pods/<pod>/total_rating`** ***[URL]*** ***[TR]*** **Tenant**

Get pods aggregated rating.

Parameters expected:

- `start`
- `end`

**GET `/pods/<pod>/metrics/<metric>/rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating for a given pod and metric.

Parameters expected:

- `start`
- `end`

**GET `/pods/<pod>/metrics/<metric>/total_rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the aggregated rating for a given pod and metric.

Parameters expected:

- `start`
- `end`

### ***NODE***

**GET `/nodes`** **Tenant**

Get the list of nodes.

**GET `/nodes/rating`** ***[TR]*** **Tenant**

Get nodes rating.

Parameters expected:

- `start`
- `end`

**GET `/nodes/total_rating`** ***[TR]*** **Tenant**

Get the nodes agglomerated rating.

Parameters expected:

- `start`
- `end`

**GET `/nodes/metrics/rating`** ***[TR]*** **Tenant**

Get the nodes metrics.

Parameters expected:

- `start`
- `end`

**GET `/nodes/<node>/<aggregator>`** ***[URL]*** **Tenant**

Get the nodes rating by time aggregation.

Available aggregator are:

- `daily`
- `weekly`
- `monthly`

**GET `/nodes/<node>/rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating for a given node.

Parameters expected:

- `start`
- `end`

**GET `/nodes/<node>/total_rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the agglomerated rating for a given node.

Parameters expected:

- `start`
- `end`

**GET `/nodes/metrics/<metric>/rating`** ***[URL]*** ***[TR]*** **Tenant**

Get the rating for a given metric.

Parameters expected:

- `start`
- `end`

## ***CONFIGS***

### ***Rating rules***

**Rating rules templates**

**GET `/templates/list`** 

Get the list of all the RatingRules templates names from the local configuration directory.

**GET `/templates/get`**

Get the RatingRule template object for a given template.
Expect a payload with:

- `query_name `

**POST `/templates/add`**

Add a new RatingRule template.

Expect a payload with:

- `query_name `
- `query_group`
- `query_template`
- `query_variables`

**POST `/templates/delete`** 

Delete a template configuration.

Expect a payload with:

- `query_name `

***Rating rules values***

**GET `templates/metric/list`** **Public**

Get the list of all the Rating Rule values.

**GET `templates/metric/get`** **Public**

Get a Rating Rule value configuration with its name.
Expect a payload with:

- `metric_name `


**POST `templates/metric/add`** 

Stroe the rating rule value configuration in a database.

Expect a payload with:

- `metric_name`
- `metric_group`
- `tamplate_name`
and the values of variables e.g. `'cpu' : 1 `


**POST `templates/metric/delete`** 

Delete a rating rule value configuration in a database.
Expect a payload with:

- `metric_name`


**POST `templates/metric/edit`** 

Edit a rating rule value configuration in a database.
Expect a payload with:

- `metric_name`
- `metric_group`
- `tamplate_name`
and the values of variables e.g. `'cpu' : 1 `


***Rating rules instance***

**POST `/instances/add`** 

Deploy the rating rule instance.

Expect a payload with:

- `metric_name`
- `template_name`
and variables values

**POST `/instances/edit`** 

edit the rating rule instance.

Expect a payload with:

- `metric-name`
- `template-name`
and variables values

**GET `/instances/list`** 

Get the list of the rating rules instances from the local configuration directory.

**GET `/instances/get`**

Get the rating rule instance object for a given instance name.
Expect a payload with:

- `metric-name`

**POST `/instances/delete`** 

Delete a rating rule instance.
Expect a payload with:

- `metric-name`



**Note that** the templates and instances modification history is stored in postgres database in the folowing tables:

1. The templates modification history:

- `id` : the timestamp of the modification
- `t_name` : the template name
- `t_group` : the template group
- `timeframe` : the duration of the query
- `t_var` :  the query variables 
- `t_query` : the query



| id                         | t_name         | t_group    |  m_var   | t_query                                                |
|----------------------------|----------------|------------|----------|--------------------------------------------------------|
| 2021-08-18 12:44:01.243088 | aws-cloud-cost | cloud cost |cpu-price | (ceil(sum(instance:node_cpu:ratio) /{cpu}) * {price}   |



2. The instances modification history:

- `id` : the timestamp of the modification
- `m_name` : the metric name
- `timeframe` : the duration of the query
- `m_var` :  the variables values
- `t_name` : the template name



| id                         | m_name         | timeframe | m_var            | t_name                |
|----------------------------|----------------|-----------|------------------|-----------------------|
| 2021-08-18 12:44:01.243088 | aws-cloud-cost |  3600s    |       1-0.5      | cloud-cost-simulation |




