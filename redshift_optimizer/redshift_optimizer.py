import sys
import os
import imp
import getpass
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

cfg_pth = os.path.dirname(os.path.abspath(__file__)) 
#IMPORTANT: pLib directory should exist in same path as redshift_optimizer direcotry 
pth_lib = os.path.dirname(cfg_pth)+'/pLib/' 
rss = imp.load_source('rs_scraper', pth_lib+'rs_scraper.py')
rsql = imp.load_source('rs_sql', pth_lib+'rs_sql.py')

class Redshift:
    def __init__(self):
        self.RFhost = 'redshift'
        self.schema = 'proddb'
        self.lookback = "-1" #how many days to lookback
    
    def getRedshiftActivity(self):
        engine = rsql.create_engine(self.RFhost, self.schema)
        # Identifying Queries That Are Top Candidates for Tuning
        # max query time greater than 10 seconds
        sql1 = """
            select trim(database) as db, count(query) as n_qry, 
            max(substring (qrytext,1,200)) as qrytext, 
            min(run_minutes)*60::decimal(12,2) as "min(s)" , 
            max(run_minutes)*60::decimal(12,2) as "max(s)", 
            avg(run_minutes)*60::decimal(12,2) as "avg(s)", 
            sum(run_minutes)::decimal(12,2) as "total(min)",  
            max(query) as max_query_id, 
            max(starttime)::date as last_run, 
            sum(alerts) as alerts, aborted
            from (select userid, label, stl_query.query, 
            trim(database) as database, 
            trim(querytxt) as qrytext, 
            md5(trim(querytxt)) as qry_md5, 
            starttime, endtime, 
            (datediff(seconds, starttime,endtime)::numeric(12,2))/60 as run_minutes,     
            alrt.num_events as alerts, aborted 
            from stl_query 
            left outer join 
            (select query, 1 as num_events from stl_alert_event_log group by query ) as alrt 
            on alrt.query = stl_query.query
            where userid <> 1 and starttime >= dateadd(day,"""+self.lookback+""", current_date)) 
            where trim(database) = '"""+self.schema+"""'
            group by database, label, qry_md5, aborted
            having max(run_minutes) >0.16
            order by "total(min)" desc;
        """
        self.df1 = pd.read_sql_query(sql1, engine)

        # Identifying Tables with Data Skew or Unsorted Rows
        # (skew > 4 (suboptimal data distribution) or pct_unsorted > 20% (run VACUUM command) or pct_of_total > 40% (more disk space needed) 
        sql2 = """
            SELECT trim(pgn.nspname) as schema,
            trim(a.name) as table, id as tableid,
            decode(pgc.reldiststyle,0, 'even',1,det.distkey ,8,'all') as distkey, 
            dist_ratio.ratio::decimal(10,4) as skew,
            det.head_sort as "sortkey",
            det.n_sortkeys as "#sks", 
            b.mbytes,
            decode(b.mbytes,0,0,((b.mbytes/part.total::decimal)*100)::decimal(5,2)) as pct_of_total,
            decode(det.max_enc,0,'n','y') as enc, a.rows,
            decode( det.n_sortkeys, 0, null, a.unsorted_rows ) as unsorted_rows ,
            decode( det.n_sortkeys, 0, null, decode( a.rows,0,0, (a.unsorted_rows::decimal(32)/a.rows)*100) )::decimal(5,2) as pct_unsorted
            from (select db_id, min(b.datname) as database, id, name, sum(rows) as rows,
            sum(rows)-sum(sorted_rows) as unsorted_rows
            from stv_tbl_perm a
            join pg_database b on (b.oid = a.db_id) 
            group by db_id, id, name) as a
            join pg_class as pgc on pgc.oid = a.id
            join pg_namespace as pgn on pgn.oid = pgc.relnamespace
            left outer join (select tbl, count(*) as mbytes
            from stv_blocklist group by tbl) b on a.id=b.tbl
            inner join (select attrelid,
            min(case attisdistkey when 't' then attname else null end) as "distkey",
            min(case attsortkeyord when 1 then attname  else null end ) as head_sort ,
            max(attsortkeyord) as n_sortkeys,
            max(attencodingtype) as max_enc
            from pg_attribute group by 1) as det
            on det.attrelid = a.id
            inner join ( select tbl, max(mbytes)::decimal(32)/min(mbytes) as ratio
            from (select tbl, trim(name) as name, slice, count(*) as mbytes
            from svv_diskusage group by tbl, name, slice )
            group by tbl, name ) as dist_ratio on a.id = dist_ratio.tbl
            join ( select sum(capacity) as  total
            from stv_partitions where part_begin=0 ) as part on 1=1
            where mbytes is not null
             AND trim(database) ='"""+self.schema+"""' 
             AND (dist_ratio.ratio > 4 
             OR decode( det.n_sortkeys, 0, null, decode( a.rows,0,0, (a.unsorted_rows/a.rows))) > 0.2 
             OR b.mbytes/part.total > 0.4) 
            order by  mbytes desc;
        """
        self.df2 = pd.read_sql_query(sql2, engine)

        # Identifying Queries with Nested Loops
        sql3 = """
            SELECT query, trim(querytxt) as querytxt, starttime
            from stl_query
            where query in (
            select distinct query
            from stl_alert_event_log
            join stl_query using (userid, query)
            where event like 'Nested Loop Join in the query plan%%')
            AND trim(database) = '"""+self.schema+"""'
            AND starttime >= dateadd(day,"""+self.lookback+""", current_Date)
            order by starttime desc;
        """
        self.df3 = pd.read_sql_query(sql3, engine)

        # Reviewing Queue Wait Times for Queries (in last 1 day)
        sql4 = """
            select trim(database) as DB , w.query, 
            substring(q.querytxt, 1, 100) as querytxt,  w.queue_start_time, 
            w.service_class as class, w.slot_count as slots, 
            w.total_queue_time/1000000 as queue_seconds, 
            w.total_exec_time/1000000 exec_seconds, (w.total_queue_time+w.total_exec_time)/1000000 as total_seconds 
            from stl_wlm_query w 
            left join stl_query q on q.query = w.query and q.userid = w.userid 
            where w.queue_start_time >= dateadd(day,"""+self.lookback+""", current_Date) 
            and trim(database) = '""" +self.schema+"""'
            and w.total_queue_time > 0  
            and w.userid >1   
            and q.starttime >= dateadd(day,"""+self.lookback+""", current_Date) 
            order by w.total_queue_time desc, w.queue_start_time desc limit 35;
        """
        self.df4 = pd.read_sql_query(sql4, engine)

        # Reviewing Query Alerts by Table
        # (total wait time greater than 1 minute)
        sql5 = """
            SELECT trim(s.perm_table_name) as table,
            (sum(abs(datediff(seconds, s.starttime, s.endtime)))/60)::numeric(24,2) as minutes, 
            trim(split_part(l.event,':',1)) as event,  
            trim(l.solution) as solution,
            max(l.query) as sample_query, count(*)
            from stl_alert_event_log as l
            join stl_query using (userid, query)
            left join stl_scan as s on s.query = l.query and s.slice = l.slice
            and s.segment = l.segment and s.step = l.step
            where l.event_time >=  dateadd(day,"""+self.lookback+""", current_Date)
            AND trim(database) = '""" + self.schema+ """'
            group by 1,3,4
            having (sum(abs(datediff(seconds, s.starttime, s.endtime)))/60) > 1
            order by 2 desc,6 desc;
        """
        self.df5 = pd.read_sql_query(sql5, engine)

        # Identifying Tables with Missing Statistics
        sql6 = """
            SELECT substring(trim(plannode),1,200) as plannode, count(*)
            from stl_explain
            join stl_query using (userid, query)
            where plannode like '%%missing statistics%%'
            AND trim(database) = '""" + self.schema+ """'
            group by plannode
            order by 2 desc;
        """
        self.df6 = pd.read_sql_query(sql6, engine)

    def emailRedshiftReport(self, recipients):
        #HOSTNAME = rss.get_hostname()
        eFrom = "query_optimizer@" + HOSTNAME
        eTo = ", ".join(recipients)
        eSubject= "Query Optimizer: "+ self.schema + "@" + HOSTNAME 
        
        #Get Data for Body of Message
        self.getRedshiftActivity()
        textL = ['http://docs.aws.amazon.com/redshift/latest/dg/diagnostic-queries-for-query-tuning.html',
            '1) Most expensive queries (in last '+self.lookback+""" day(s)):
                <ul> 
                <li> wait time (max) greater than 10 seconds </li>
                </ul>""",
            """2) Tables with data skew or unsorted rows: 
                <ul> 
                <li> skew > 4 (suboptimal data distribution) </li> 
                <li> pct_unsorted > 20% (run VACUUM command) </li> 
                <li> pct_of_total > 40% (more disk space required) </li>
                </ul>""",
            '3) Queries with nested loops (in last '+self.lookback+' day(s)):',
            '4) Queries with nonzero queue times (in last '+self.lookback+""" day(s)):
                <ul> 
                <li> Total wait time (total_seconds) = queue time + execution time</li>
                </ul>""",
            '5) Query alerts by table (in last '+self.lookback+""" day(s)):
                <ul> 
                <li> Total wait time greater than 1 minute</li>
                </ul>""",
            '6) Tables with missing statistics (run ANALYZE command):']
        
        # Create the body of the message (a plain-text and an HTML version).
        pd.options.display.max_colwidth = 100
        eHTML = """\
        <html>
          <head></head>
          <body>
        """ + '<h2><a href="'+textL[0]+'"> Query Optimization </a></h2>'\
            + '<h3>' + textL[1] + '</h3>' \
            + self.df1.to_html(justify='left') \
            + '<h3>' + textL[2] + '</h3>' \
            + self.df2.to_html(justify='left') \
            + '<h3>' + textL[3] + '</h3>' \
            + self.df3.to_html(justify='left') \
            + '<h3>' + textL[4] + '</h3>' \
            + self.df4.to_html(justify='left') \
            + '<h3>' + textL[5] + '</h3>' \
            + self.df5.to_html(justify='left') \
            + '<h3>' + textL[6] + '</h3>' \
            + self.df6.to_html(justify='left') \
        + """ 
          </body>
        </html>
        """
        rss.send_email(eFrom, eTo, eSubject, eHTML)

if __name__ == '__main__':
    rf = Redshift()
    recipients = ['email1@host.com','email2@host.com'] #Enter recipient emails here
    rf.emailRedshiftReport(recipients)
