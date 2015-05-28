import datetime
# import subprocess
import time
from pyLibrary.debugs.logs import Log


def strip_branch(buildername):
    #parse buildername, ignore branch (i.e. merges)
    retVal = buildername
    branches = ['fx-team', 'mozilla-inbound', 'mozilla-central', 'mozilla-aurora', 'try']
    for branch in branches:
        retVal = retVal.replace(branch, '')
    return retVal


def filter_instances(instances, action_mode=False):
    retVal = []
    oldest = Date.now()
    oldinstance = {'buildname': ''}
    for instance in instances:
        instance['oldest'] = False

        # NOTE: I am hacking out these specific job types until we have a working system
        if 'talos' in instance['buildname']:
            continue
        if 'b2g' in instance['buildname']:
           continue
        if 'ndroid' in instance['buildname']:
            continue
        if 'mozmill' in instance['buildname']:
            continue
        if 'cppunit' in instance['buildname']:
            continue
        if 'try' in instance['buildname']:
            continue
        if action_mode:
            if 'ASAN' in instance['buildname']:
                continue
            if 'pgo' in instance['buildname']:
                continue
            # too new of a platform
            if 'Yosemite' in instance['buildname']:
               continue
            # just turned off platform
            if 'Mountain' in instance['buildname']:
               continue

        curtime = datetime.datetime.strptime(instance['timestamp'], '%Y-%m-%dT%H:%M:%S')
        if curtime < oldest:
            oldest = curtime
            if oldinstance in retVal:
                retVal.remove(oldinstance)
            oldinstance = instance
            instance['oldest'] = True

        found = False
        if action_mode:
            if curtime >= oldest:
                if strip_branch(instance['buildname']) == strip_branch(oldinstance['buildname']):
                    continue

            for item in retVal:
                if strip_branch(instance['buildname']) == strip_branch(item['buildname']):
                    found = True
                    break

        if not found:
            retVal.append(instance)

    return retVal



def intermittent_opened_count_last_week():
    tday = datetime.date.today()
    tday_minus_7 = tday - datetime.timedelta(days=90)
    today = '%s-%s-%s' %(tday.year, tday.month if tday.month >= 10 else '0%s' % tday.month, tday.day)
    seven_days_ago = '%s-%s-%s' %(tday_minus_7.year, tday_minus_7.month if tday_minus_7.month >= 10 else '0%s' % tday_minus_7.month, tday_minus_7.day)
    bugzilla = bugsy.Bugsy()
    bugs = bugzilla.search_for\
                .keywords("intermittent-failure")\
                .change_history_fields(['[Bug creation]'])\
                .timeframe(seven_days_ago, today)\
                .search()

    for bug in bugs:
#        if str(bug.id) != '1113543':
#            continue

        try:
            comments = bug.get_comments()
        except:
            print "issue getting comments for bug %s" % bug.id
            comments = []
        instances = []
        for comment in comments:
            instances.append(parse_comment(bug, comment))

        instances = filter_instances(instances)
        if len(instances):
            for instance in instances:
                star = ""
                if instance['oldest']:
                    star = "*"
#                print "%s\t%s:%s - %s" % (star, instance['revision'], instance['timestamp'], instance['buildname'])
                if star == "*":
                    cmd = "python trigger.py --rev %s --back-revisions 30 --times 30 --skip 15 --buildername \"%s\"" % (instance['revision'], instance['buildname'])
                    timestamps = [x['timestamp'] for x in instances]
                    timestamps.sort()
                    time_to_second = 0
                    time_to_last = 0
                    if len(timestamps) > 1:
                        start = datetime.datetime.strptime(timestamps[0], '%Y-%m-%dT%H:%M:%S')
                        next = datetime.datetime.strptime(timestamps[1], '%Y-%m-%dT%H:%M:%S')
                        last = datetime.datetime.strptime(timestamps[-1], '%Y-%m-%dT%H:%M:%S')
                        time_to_second = (next - start).total_seconds()
                        time_to_last = (last - start).total_seconds()
                    print '%s,%s,%s,%s,%s'   % (bug.id, len(instances), int(time_to_second / 60), int(time_to_last / 60), ','.join(timestamps))
#                    thlink = findTHLink(cmd)
#                    print "%s, %s, %s, 0" % (bug.id, cmd, thlink)

    return bugs

