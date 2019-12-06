const Debug = require('debug');
const taskcluster = require('taskcluster-client');
const _ = require('lodash');
const {Task} = require('./task');
const {consume} = require('taskcluster-lib-pulse');

let events = new taskcluster.QueueEvents({
  rootUrl: process.env.TASKCLUSTER_ROOT_URL,
});
let debug = Debug('task-analysis:handler');

const EVENT_MAP = {
  [events.taskPending().exchange]: 'pending',
  [events.taskRunning().exchange]: 'running',
  [events.taskCompleted().exchange]: 'completed',
  [events.taskFailed().exchange]: 'failed',
  [events.taskException().exchange]: 'exception',
};


class Handler {
  constructor(options) {
    this.queue = options.queue;
    this.pulseClient = options.pulseClient;
    this.db = options.db;
    this.taskQueueName = options.taskQueueName;
  }

  async start() {
    debug('Starting handler');

    let routingPattern = `#`;
    await consume({
      client: this.pulseClient,
      bindings: [
        events.taskPending(routingPattern),
        events.taskRunning(routingPattern),
        events.taskCompleted(routingPattern),
        events.taskFailed(routingPattern),
        events.taskException(routingPattern),
      ],
      queueName: this.taskQueueName,
    }, this.handleMessage.bind(this));
    debug('Started Handler');
  }

  async getTaskDefinition(taskId) {
    let res = await this.db.query(
      'SELECT definition from cached_task_definitions where task_id = $1',
      [taskId]
    );

    if (res.rowCount === 1) {
      return res.rows[0].definition;
    }

    let def = await this.queue.task(taskId);
    await this.db.query(
      'INSERT INTO cached_task_definitions (task_id, definition)' +
      ' VALUES ($1, $2)',
      [taskId, JSON.stringify(def)]
    );
    return def;
  }

  async handleMessage(message) {
    let taskId = message.payload.status.taskId;

    debug(`message received for task ${taskId} with state ${EVENT_MAP[message.exchange]}`);

    let taskDef = await this.getTaskDefinition(taskId);

    let task = new Task(message, taskDef);

    switch (EVENT_MAP[message.exchange]) {
      case 'pending':
        return await this.handleTaskPending(task);
      case 'running':
        return await this.handleTaskRunning(task);
      case 'completed':
        return await this.handleTaskCompleted(task);
      case 'failed':
        return await this.handleTaskCompleted(task);
      case 'exception':
        return await this.handleTaskException(task);
      default:
        throw new Error(`Unknown exchange: ${message.exchange}`);
    }
  }

  async handleTaskPending(task) {
    // Task runs that were created due to automatic rerun (such as some task-exceptions)
    // should have the previous run resolved properly.  Task exception events are
    // not published when a task is rerun.
    if (task.runId > 0 && task.currentRun.reasonCreated === 'retry') {
      await this.handleTaskRetry(task);
    }

    await this.db.query(
      'INSERT INTO tasks' +
      ' (task_id, run_id, state, created, scheduled, source, owner, project,' +
      ' revision, push_id, scheduler, provisioner, worker_type, platform, job_kind)' +
      ' VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)' +
      // In case a duplicate pending message is received, ignore the message and
      // do nothing
      ' ON CONFLICT DO NOTHING',
      [
        task.taskId,
        task.runId,
        task.currentRun.state,
        task.taskStatus.created,
        task.currentRun.scheduled,
        task.source.origin,
        task.source.owner,
        task.source.project,
        task.source.revision,
        task.source.pushId,
        task.taskStatus.schedulerId,
        task.taskStatus.provisionerId,
        task.taskStatus.workerType,
        task.platform,
        task.jobKind,
      ]
    );

    return;
  }

  async handleTaskRetry(task) {
    let runId = task.runId-1;
    let currentRun = task.runs[runId];
    let start = new Date(currentRun.scheduled);
    if (currentRun.started) {
      start = new Date(currentRun.started);
    }

    await this.db.query(
      'INSERT INTO tasks' +
      ' (task_id, run_id, state, created, scheduled, source, owner, project,' +
      ' revision, push_id, scheduler, provisioner, worker_type, platform, job_kind,' +
      ' worker_id, worker_group, started, resolved, exception_reason, duration)' +
      ' VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)' +
      ' ON CONFLICT ON CONSTRAINT dup_task_run DO UPDATE' +
      ' SET state=EXCLUDED.state, worker_id=EXCLUDED.worker_id, worker_group=EXCLUDED.worker_group,' +
      ' scheduled=EXCLUDED.scheduled, started=EXCLUDED.started, ' +
      ' resolved=EXCLUDED.resolved, exception_reason=EXCLUDED.exception_reason, ' +
      ' duration=EXCLUDED.duration',
      [
        task.taskId,
        runId,
        currentRun.state,
        task.taskStatus.created,
        currentRun.scheduled,
        task.source.origin,
        task.source.owner,
        task.source.project,
        task.source.revision,
        task.source.pushId,
        task.taskStatus.schedulerId,
        task.taskStatus.provisionerId,
        task.taskStatus.workerType,
        task.platform,
        task.jobKind,
        currentRun.workerId,
        currentRun.workerGroup,
        currentRun.started,
        currentRun.resolved,
        currentRun.reasonResolved,
        new Date(currentRun.resolved) - start,
      ]
    );

    return;
  }

  async handleTaskRunning(task) {
    await this.db.query(
      'INSERT INTO tasks' +
      ' (task_id, run_id, state, created, scheduled, source, owner, project, ' +
      ' revision, push_id, scheduler, provisioner, worker_type, platform, job_kind, worker_id, worker_group, started)' +
      ' VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)' +
      ' ON CONFLICT ON CONSTRAINT dup_task_run DO UPDATE' +
      ' SET scheduled=EXCLUDED.scheduled, state=EXCLUDED.state, started=EXCLUDED.started,' +
      ' worker_id=EXCLUDED.worker_id, worker_group=EXCLUDED.worker_group',
      [
        task.taskId,
        task.runId,
        task.currentRun.state,
        task.taskStatus.created,
        task.currentRun.scheduled,
        task.source.origin,
        task.source.owner,
        task.source.project,
        task.source.revision,
        task.source.pushId,
        task.taskStatus.schedulerId,
        task.taskStatus.provisionerId,
        task.taskStatus.workerType,
        task.platform,
        task.jobKind,
        task.currentRun.workerId,
        task.currentRun.workerGroup,
        task.currentRun.started,
      ]
    );

    return;
  }

  async handleTaskCompleted(task) {
    await this.db.query(
      'INSERT INTO tasks' +
      ' (task_id, run_id, state, created, scheduled, source, owner, project,' +
      ' revision, push_id, scheduler, provisioner, worker_type, platform,' +
      ' job_kind, worker_id, worker_group, started, resolved, duration)' +
      ' VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)' +
      ' ON CONFLICT ON CONSTRAINT dup_task_run DO UPDATE' +
      ' SET scheduled=EXCLUDED.scheduled, state=EXCLUDED.state, ' +
      ' worker_id=EXCLUDED.worker_id, worker_group=EXCLUDED.worker_group, started=EXCLUDED.started,' +
      ' resolved=EXCLUDED.resolved, duration=EXCLUDED.duration',
      [
        task.taskId,
        task.runId,
        task.currentRun.state,
        task.taskStatus.created,
        task.currentRun.scheduled,
        task.source.origin,
        task.source.owner,
        task.source.project,
        task.source.revision,
        task.source.pushId,
        task.taskStatus.schedulerId,
        task.taskStatus.provisionerId,
        task.taskStatus.workerType,
        task.platform,
        task.jobKind,
        task.currentRun.workerId,
        task.currentRun.workerGroup,
        task.currentRun.started,
        task.currentRun.resolved,
        new Date(task.currentRun.resolved) - new Date(task.currentRun.started),
      ]
    );

    return;
  }

  async handleTaskException(task) {
    let start = new Date(task.currentRun.scheduled);
    if (task.currentRun.started) {
      start = new Date(task.currentRun.started);
    }

    await this.db.query(
      'INSERT INTO tasks' +
      ' (task_id, run_id, state, created, scheduled, source, owner, project,' +
      ' revision, push_id, scheduler, provisioner, worker_type, platform, job_kind,' +
      ' worker_id, worker_group, started, resolved, exception_reason, duration)' +
      ' VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)' +
      ' ON CONFLICT ON CONSTRAINT dup_task_run DO UPDATE' +
      ' SET state=EXCLUDED.state, worker_id=EXCLUDED.worker_id, worker_group=EXCLUDED.worker_group,' +
      ' scheduled=EXCLUDED.scheduled, started=EXCLUDED.started, ' +
      ' resolved=EXCLUDED.resolved, exception_reason=EXCLUDED.exception_reason, ' +
      ' duration=EXCLUDED.duration',
      [
        task.taskId,
        task.runId,
        task.currentRun.state,
        task.taskStatus.created,
        task.currentRun.scheduled,
        task.source.origin,
        task.source.owner,
        task.source.project,
        task.source.revision,
        task.source.pushId,
        task.taskStatus.schedulerId,
        task.taskStatus.provisionerId,
        task.taskStatus.workerType,
        task.platform,
        task.jobKind,
        task.currentRun.workerId,
        task.currentRun.workerGroup,
        task.currentRun.started,
        task.currentRun.resolved,
        task.currentRun.reasonResolved,
        new Date(task.currentRun.resolved) - start,
      ]
    );
    return;
  }
}

module.exports = {Handler};
