const API = require('taskcluster-lib-api');

let api = new API({
  title:        'TaskCluster Task Analysis Documentation',
  description: [
    'Taskcluster Task Analysis is a service used to capture task events',
    'and persist them to a database for analysis.  This service can changed and is',
    'only experimentally supported',
  ].join('\n'),
  schemaPrefix: 'http://schemas.taskcluster.net/taskcluster-task-analysis/v1/',
  context: ['cfg', 'db'],
});

module.exports = api;

/** Workers for Worker Group **/
api.declare({
  name:   'listWorkerGroup',
  method: 'get',
  route:  '/worker-groups/:workerGroup/workers',
  query: {
    limit: /^[0-9]+$/,
  },
  output: 'list-worker-group-response.json#',
  stability: API.stability.experimental,
  title: 'List Worker Group',
  description: 'List worker IDs sharing the same `workerGroup` ID',
}, async function(req, res) {
  let workerGroup = req.params.workerGroup;
  let limit = parseInt(req.query.limit || 100, 10);

  let data = await this.db.query(
    'SELECT distinct(worker_id), started from tasks where worker_group = $1 order by started desc limit $2',
    [workerGroup, limit],
  );

  let result = {
    workerGroup,
    workers: [],
  };
  data.rows.forEach(r => {result.workers.push({workerId: r.worker_id});});

  return res.reply(result);
});

/** Worker Information **/
api.declare({
  name: 'worker',
  method: 'get',
  route: '/worker-groups/:workerGroup/workers/:workerId/tasks',
  output: 'describe-worker-response.json#',
  stability: API.stability.experimental,
  title: 'Describe Worker',
  description: 'List details known about a given worker ID',
}, async function(req, res) {
  let workerGroup = req.params.workerGroup;
  let workerId = req.params.workerId;
  // We should only need to return the last 100 tasks to know meaningful information
  // about a worker.  Perhaps later we can make this a query param with offset.
  let taskLimit = 20;

  let data = await this.db.query(
    'SELECT * from tasks where worker_id = $1 and worker_group = $2 order by started desc limit $3',
    [workerId, workerGroup, taskLimit]
  );

  let result = {
    workerGroup,
    workerId,
    tasks: [],
  };
  data.rows.forEach(r => {
    let newTask = {};
    Object.keys(r).forEach(k => {
      let v = r[k];
      let newKey = k.replace(/_([a-z])/g, (g) => {return g[1].toUpperCase();});
      newTask[newKey] = v;
    });
    result.tasks.push(newTask);
  });
  return res.reply(result);
});
