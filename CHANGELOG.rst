5.0.25 (2021-03-15)
-------------------

- Set guillotina.AccessContent permission to GET task
  [qiwn]


5.0.24 (2021-02-08)
-------------------

- More metrics on job execution

5.0.23 (2021-02-04)
-------------------

- Lower default number of max_running_tasks, as each task is
  potentially a different connection to the database.

- Allow to retry tasks forever

5.0.22 (2020-10-15)
-------------------

- Add metrics on jobs, statuses, amqp and redis
  [vangheem]


5.0.21 (2020-09-22)
-------------------

- Fix getting metric
  [vangheem]

5.0.20 (2020-03-09)
-------------------

- Ack canceled tasks so they are cleaned up from the main queue
  [lferran]


5.0.19 (2020-03-05)
-------------------

- Filter job_data in task info if the requester is not root [acatlla]


5.0.18 (2020-02-28)
-------------------

- Handle KeyError when attempting to queue tasks but amqp settings are
  not present [lferran]

5.0.17 (2020-02-15)
-------------------

- Fix unsafe thread operation: creating asyncio task from another thread
  [vangheem]

5.0.16 (2020-01-31)
-------------------

- Fix after commit handler [vangheem]


5.0.15 (2019-12-23)
-------------------

- Retry rabbitQM TimeoutErrors [lferran]

5.0.14 (2019-12-20)
-------------------

- bump


5.0.13 (2019-12-20)
-------------------

- Retry ConnectionResetError on redis [lferran]

5.0.12 (2019-11-14)
-------------------

- Retry if rabbitmq is down when getting new connections [lferran]


5.0.11 (2019-11-05)
-------------------

- add py.typed to manifest


5.0.10 (2019-11-05)
-------------------

- bump


5.0.9 (2019-11-01)
------------------

- Be able to import types


5.0.8 (2019-10-31)
------------------

- Schedule NOOP tasks from worker to prevent channel from hanging
  [lferran]

5.0.7 (2019-10-29)
------------------

- Do not exit when tasks are running


5.0.6 (2019-10-28)
------------------

- safe exit


5.0.5 (2019-10-27)
------------------

- Better handling in tests
  [vangheem]


5.0.4 (2019-10-27)
------------------

- Restart worker if no connection activity in last 5 minutes [lferran]

5.0.3 (2019-10-25)
------------------

- Removed beacon system [lferran]


5.0.2 (2019-09-03)
------------------

- Exit worker on error starting up.
  [vangheem]


5.0.1 (2019-09-02)
------------------

- Fix self.request
  [qiwn]


5.0.0 (2019-08-30)
------------------

- Upgrade to guillotina 5


3.1.5 (2019-06-18)
------------------

- restrict compat g version


3.1.4 (2019-06-10)
------------------

- Copy all headers from original request to task
  [vangheem]


3.1.3 (2019-06-06)
------------------

- Log exception with unhandled task errors
  [vangheem]


3.1.1 (2019-05-22)
------------------

- Include actual callable in metrics labels


3.1.0 (2019-05-21)
------------------

- Optionally serve /metrics for prometheus metrics


3.0.5 (2019-05-14)
------------------

- Fix metrics


3.0.4 (2019-05-14)
------------------

- Add container id label in metric [lferran]


3.0.3 (2019-05-14)
------------------

- Add prometheus metrics [lferran]


3.0.2 (2019-05-14)
------------------

- Provide b/w compatible `@amqp-info` and `@amqp-cancel` endpoints. Marked for complete
  removal in version 4.
  [vangheem]


3.0.0 (2019-05-13)
------------------

- Rename `DELETE @amqp-cancel/{task_id}` to `DELETE @amqp-tasks/{task_id}`
  [vangheem]

- Rename `GET @amqp-info/{task_id}` to `GET @amqp-tasks/{task_id}`
  [vangheem]

- API methods should be constrained to only work against a container
  [vangheem]


2.2.7 (2019-04-29)
------------------

- Make sure `max_running_tasks` is always an integer
  [vangheem]


2.2.6 (2019-04-16)
------------------

- Fix releasing task
  [vangheem]


2.2.5 (2019-04-16)
------------------

- Fix issue where tasks would never be scheduled or consuming
  would be extremely slow
  [vangheem]


2.2.4 (2019-04-16)
------------------

- Fix spamming when waiting for tasks to finish
  [vangheem]

2.2.3 (2019-04-04)
------------------

- Do not retry tasks for objects that are no longer in the database
  [lferran]

2.2.2 (2019-03-08)
------------------

- Bugfix: make prefetch count match the configured max running tasks
  [lferran]

2.2.1 (2019-03-08)
------------------

- Use glogging [lferran]


2.2.0 (2019-02-27)
------------------

- Expire finished and errored tasks
- Make max running tasks parameter configurable
- Refactor way we update task states


2.1.0 (2019-02-04)
------------------

- Make sure that abort is run after job failure
  [vangheem]

- Execute `request.execute_futures()` after successful
  [vangheem]

- Added custom permission for amqp endpoints and assigned to
  `guillotina.Manager` role by default [lferran]

- Configurable ttls for delay and error queues [lferran]

2.0.3 (2018-12-19)
------------------

- Fix publish_beacon_to_delay_queue call sig error
  [vangheem]


2.0.2 (2018-12-06)
------------------

- Fix guillotina_rediscache constraint
  [vangheem]


2.0.1 (2018-12-04)
------------------

Bugfix:

- support async generators for object tasks aswell [lferran]

- Don't use globals for the beacons liveness system and move the code
  to the BeaconsManager class (1 manager per connection) [davidonna]


2.0.0
-----

Major improvements:

 - Added task retrial using delay queue
 - Tasks are only ACKed if successful, otherwise are sent to delay queue
 - Allow task cancelation
 - Improved API
 - Upgraded to guillotina 4
 - Added plenty of tests for worker, amqp and state manager


1.0.8 (2018-10-09)
------------------

- Retry on conflict error
  [vangheem]


1.0.7 (2018-10-08)
------------------

- Provide `@task-status/{id}` endpoint
  [vangheem]

- Fix port references
  [vangheem]


1.0.6 (2018-06-15)
------------------

- Fix
  [vangheem]


1.0.5 (2018-06-15)
------------------

- Be able to add tasks after request and commit
  [vangheem]


1.0.4 (2018-06-13)
------------------

- Copy request annotation data over as well
  [vangheem]


1.0.3 (2018-06-13)
------------------

- Fix serialization issues with roles

- Handle invalid state manager


1.0.2 (2018-06-13)
------------------

- Fix again


1.0.1 (2018-06-13)
------------------

- Really release


1.0.0 (2018-06-13)
------------------

- initial
