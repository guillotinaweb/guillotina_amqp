from guillotina import configure


# Add new permission
configure.permission("guillotina.ManageAMQP", "Manage guillotina amqp endpoints")
configure.permission("guillotina.DebugAMQP", "Debug guillotina amqp tasks")

# Grant it to guillotina.Manager
configure.grant(permission="guillotina.ManageAMQP", role="guillotina.Manager")
configure.grant(permission="guillotina.DebugAMQP", role="guillotina.Manager")
