
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
