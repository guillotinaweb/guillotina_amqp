from guillotina import configure


# Add new permission
configure.permission('guillotina.ManageAMQP', 'Manage guillotina amqp endpoints')

# Grant it to guillotina.Manager
configure.grant(
    permission='guillotina.ManageAMQP',
    role='guillotina.Manager')
