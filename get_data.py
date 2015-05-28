from pyLibrary import convert
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, set_default
from pyLibrary.env import http, big_data
from pyLibrary.env.files import File
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import WEEK

RECENT = Date.today() - 1 * WEEK


blacklist = [
        'automation.py',
        'remoteautomation.py',
        'Shutdown',
        'undefined',
        'Main app process exited normally',
        'Traceback (most recent call last):',
        'Return code: 0',
        'Return code: 1',
        'Return code: 2',
        'Return code: 9',
        'Return code: 10',
        'Exiting 1',
        'Exiting 9',
        'CrashingThread(void *)',
        'libSystem.B.dylib + 0xd7a',
        'linux-gate.so + 0x424',
        'TypeError: content is null',
        'leakcheck',
        'command timed out:',
        'build failure:',
        "Graph server unreachable",
        "All talos on OSX fails with running in e10s mode",
        "mozprocess timed out after",
        "find: Filesystem loop detected",
        "TC build failure",
        "NameError: global name ",
        "seconds without output, attempting to kill",
        "application crashed"
    ]

def get_active_data(settings):
    query = {
    "limit": 1000,
    "from": "unittest",
    "where": {"and": [
        {"eq": {"result.ok": False}},
        {"gt": {"run.timestamp": "{{today-week}}"}}
    ]},
    "select": [
        "result.ok",
        "build.branch",
        "build.platform",
        "build.release",
        "build.revision",
        "build.type"
        "build.revision",
        "run.suite",
        "run.chunk",
        "result.test",
        "run.stats.status.test_status"
    ],
    "format": "table"
    }
    result = http.post("http://activedata.allizom.org/query", data=convert.unicode2utf8(convert.value2json(query)))

    query_result = convert.json2value(result.all_content)

    tab = convert.table2tab(query_result.header, query_result.data)
    File(settings.output.activedata).write(tab)



def get_bugs(settings):
    request_bugs = {
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and": [
                {"term": {"keyword": "intermittent-failure"}},
                {"range": {"expires_on": {"gt": Date.now().milli}}},
                {"range": {"modified_ts": {"gt": RECENT.milli}}}
            ]}
        }},
        "from": 0,
        "size": 200000,
        "sort": [],
        "facets": {},
        "fields": ["bug_id", "bug_status", "short_desc", "status_whiteboard"]
    }
    bugs = UniqueIndex(["bug_id"], convert.json2value(convert.utf82unicode(http.post(settings.bugs.url, data=request_bugs).all_content)).hits.hits.fields)

    for i, b in enumerate(bugs):
        try:
            parse_short_desc(b)
        except Exception, e:
            Log.warning("can not parse {{bug_id}} {{short_desc}}", bug_id=b.bug_id, short_desc=b.short_desc, cause=e)

    request_comments = convert.unicode2utf8(convert.value2json({
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and":[
                {"terms": {"bug_id": bugs.keys()}},
                {"range": {"modified_ts": {"gt": RECENT.milli}}}
            ]}
        }},
        "from": 0,
        "size": 200000,
        "sort": [],
        "facets": {},
        "fields": ["bug_id", "modified_by", "modified_ts", "comment"]
    }))

    comments = convert.json2value(convert.utf82unicode(http.post(settings.comments.url, data=request_comments).all_content)).hits.hits.fields

    results = []
    for c in comments:
        errors = parse_comment(bugs[c.bug_id], c)
        results.extend(errors)

    tab = convert.list2tab(results)
    File(settings.output.tab).write(tab)



def parse_short_desc(bug):
    parts = bug.short_desc.split("|")
    if len(parts) in [2, 3]:
        bug.result.test = parts[0].strip()
        bug.result.message = parts[1].strip()
    elif any(map(parts[0].strip().endswith, [".html", ".py", ".js", ".xul"])) and len(parts)>2:
        bug.result.test = parts[0].strip()
        bug.result.message = parts[1].strip()
    elif len(parts) >= 4:
        set_default(bug.result, parse_status(parts[0]))
        bug.result.test = parts[1].strip()
        bug.result.message = parts[3].strip()
    elif any(black in bug.short_desc for black in blacklist):
        Log.note("IGNORED {{line}}", line=bug.short_desc)
    elif bug.bug_id in [1165765]:
        Log.note("IGNORED {{line}}", line=bug.short_desc)
    elif "###" in bug.short_desc:
        bug.short_desc = bug.short_desc.replace("###", " | ")
        parse_short_desc(bug)
    else:
        Log.alert("can not handle {{bug_id}}: {{line}}", line=bug.short_desc, bug_id=bug.bug_id)

    if bug.result.test.lower().startswith("intermittent "):
        bug.result.test = bug.result.test[13:]

def parse_comment(bug, comment):
    subtests = []
    lines = comment.comment.split('\n')
    for line in lines:
        if not line.strip():
            continue
        elif line.startswith('log: https://treeherder.mozilla.org'):
            bug.treeherder = line.split('log: ')[1]
            continue
        elif line.startswith('buildname'):
            bug.build.name = line.split('buildname: ')[1]
            continue
        elif line.startswith('repository: '):
            bug.branch.name = line.split('repository: ')[1]
            continue
        elif line.startswith('machine: '):
            bug.machine.name = line.split('machine: ')[1]
            continue
        elif line.startswith('who: '):
            continue
        elif line.startswith('revision'):
            try:
                bug.build.revision = line.split('revision: ')[1]
                continue
            except:
                Log.error("exception splitting bug {{bug_id}} line on 'revision: ', {{line}}", bug_id=bug.id, line=line)
        elif line.startswith('start_time'):
            bug.timestamp = Date(line.split('start_time: ')[1])
            continue
        elif line.startswith('submit_timestamp'):
            bug.timestamp = line.split('submit_timestamp: ')[1]
            continue


        parts = line.split("|")

        if len(parts) == 3:
            try:
                subtest = Dict()
                subtest.subtest = parse_status(parts[0])
                subtest.subtest.name = parts[1].strip()
                subtest.subtest.message = parts[2].strip()
                set_default(subtest, bug)
                subtest.subtest.comment_line = line
                subtest.subtest.report_ts = Date(comment.modified_ts)
                subtests.append(subtest)
            except Exception, e:
                Log.note("IGNORED LINE {{bug_id}} {{line}}", line=line, bug_id=bug.bug_id)
        else:
            Log.note("IGNORED LINE {{bug_id}} {{line}}", line=line, bug_id=bug.bug_id)

    return subtests

def parse_status(status):
    special = status.upper().replace("  ", " ").strip()
    if special.endswith("TEST-UNEXPECTED-FAIL"):
        return {
            "status": "fail",
            "expected": "pass"
        }
    elif special.endswith("TEST-UNEXPECTED-TIMEOUT"):
        return {
            "status": "timeout",
            "expected": "pass"
        }
    elif special.endswith("TEST-UNEXPECTED-ERROR"):
        return {
            "status": "fail",
            "expected": "pass"
        }
    elif any(map(special.endswith, ["INFO - WARNING", "INFO - PROCESS", "INFO -  INFO", "INFO - INFO"])):
        Log.error("not a subtest")

    parts = [s.strip().lower() for s in status.split("-")]
    if parts == ['process', 'crash']:
        return {"status": "crash"}
    if len(parts) == 3:
        return {
            "status": parts[2].strip().lower(),
            "expected": "pass"
        }
    else:
        Log.error("do not know how to parse status {{status}}", status=status)



def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        big_data.MAX_STRING_SIZE = 100 * 1000 * 1000

        get_active_data(settings)
        get_bugs(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


