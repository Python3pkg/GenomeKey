#!/usr/bin/env python
from cosmos.Job.models.jobattempt import JobAttempt
from cosmos.Job.models.jobmanager_local import JobManager
from cosmos.Workflow.models import Stage, Task, TaskEdge, TaskFile, TaskTag, Workflow
from django.conf import settings
from django.contrib.admin.models import LogEntry
from django.contrib.auth.models import Group, Permission, User
from django.contrib.contenttypes.models import ContentType
from django.contrib.sessions.models import Session
from django.contrib.sites.models import Site
from django.core.cache import cache
from django.core.urlresolvers import reverse
from django.db import transaction
from django.db.models import Avg, Count, F, Max, Min, Q, Sum
from django.utils import timezone

# always get the first workflow
my_workflow = Workflow.objects.all()[0]

w_created = my_workflow.created_on
w_finished = my_workflow.finished_on
w_name = my_workflow.name

print "run,stage,status,start,stop,cpu_time,wall_time,percent_cpu,system_time,user_time"
 
for task in my_workflow.tasks:
    fields = "\"%s\",\"%s\",\"%s\"" % (w_name, task.stage.name, task.status)
    job = task.jobattempt_set.all()
    if job:
        for ja in job:
            print "%s,%s,%s,%s,%s,%s,%s,%s" % \
                  (fields,
                   (ja.created_on - w_created).total_seconds(),
                   (ja.finished_on - w_created).total_seconds(),
                   ja.cpu_time,
                   ja.wall_time,
                   ja.percent_cpu,
                   ja.system_time,
                   ja.user_time)
    else:
        print "%s,0,0,0,0,0,0,0" % fields


