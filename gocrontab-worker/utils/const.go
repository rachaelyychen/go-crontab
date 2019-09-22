package utils

const LOGSTR = "[%s][%s]"
const CAP = 500
const LEASE_TIME = 5
const BATCH = 50
const JOB_SAVE_PATH = "/cron/job/"
const JOB_KILL_PATH = "/cron/killer/"
const JOB_LOCK_PATH = "/cron/lock/"
const JOB_WORKER_PATH = "/cron/worker/"
const JOB_SAVE_EVENT = 0
const JOB_DEL_EVENT = 1
const JOB_KILL_EVENT = 2
const MON_DATABASE_CRON = "crontab"
const MON_COLLECTION_LOG = "job_log"
