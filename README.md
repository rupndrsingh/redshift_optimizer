# redshift_optimizer
Diagnostic queries for query tuning on Redshift

This script can be used to regulary email a redshift tuning report. The queries used were inspired by this post:
https://docs.aws.amazon.com/redshift/latest/dg/diagnostic-queries-for-query-tuning.html

Before running /usr/bin/python2.7 redshift_optimizer.py:
1) Update "recipient" variable in redshift_optimizer.py with recipient emails.
2) Create "~/.my.cnf" and enter in redshift connection details so they can be used by rs_sql.py:

    [redshift]<br />
    host="enter_hostname"<br />
    user="enter_username"<br />
    password="enter_password" 
