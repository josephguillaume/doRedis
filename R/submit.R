#        __      ____           ___     
#   ____/ /___  / __ \___  ____/ (_)____
#  / __  / __ \/ /_/ / _ \/ __  / / ___/
# / /_/ / /_/ / _, _/  __/ /_/ / (__  ) 
# \__,_/\____/_/ |_|\___/\__,_/_/____/  
#                                      
# Copyright (c) 2010 by Bryan W. Lewis.
# Changes for submitting single job Copyright (c) 2014 by Joseph Guillaume
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
# USA

submit <- function(expr, queue, obj=list(verbose=FALSE), envir=parent.frame())
{
  expr=substitute(expr)
  
  # ID associates the work with a job environment <queue>:<ID>.env. If
  # the workers current job environment does not match job ID, they retrieve
  # the new job environment data from queueEnv and run workerInit.
  queueCounter <- sprintf("%s:counter", queue)   # job task ID counter
  ID <- redisIncr(queueCounter)
  queueEnv <- sprintf("%s:%.0f.env",queue,ID) # R job environment
  queueTasks <- sprintf("%s:%.0f",queue,ID) # Job tasks hash
  queueStart <- sprintf("%s:%.0f.start*",queue,ID)
  queueAlive <- sprintf("%s:%.0f.alive*",queue,ID)
  
  # Setup the job parent environment
  exportenv <- tryCatch({
    qargs <- quote(list(...))
    args <- eval(qargs, envir)
    environment(do.call(.makeDotsEnv, args))
  },
  error=function(e) {
    new.env(parent=emptyenv())
  })
  noexport <- union(obj$noexport, obj$argnames)
  getexports(expr, exportenv, envir, bad=noexport)
  vars <- ls(exportenv)
  if (obj$verbose) {
    if (length(vars) > 0) {
      cat('automatically exporting the following objects',
          'from the local environment:\n')
      cat(' ', paste(vars, collapse=', '), '\n')
    } else {
      cat('no objects are automatically exported\n')
    }
  }
  # Compute list of variables to export
  export <- unique(c(obj$export,.doRedisGlobals$export))
  ignore <- intersect(export, vars)
  if (length(ignore) > 0) {
    warning(sprintf('already exporting objects(s): %s',
                    paste(ignore, collapse=', ')))
    export <- setdiff(export, ignore)
  }
  # Add explicitly exported variables to exportenv
  if (length(export) > 0) {
    if (obj$verbose)
      cat(sprintf('explicitly exporting objects(s): %s\n',
                  paste(export, collapse=', ')))
    for (sym in export) {
      if (!exists(sym, envir, inherits=TRUE))
        stop(sprintf('unable to find variable "%s"', sym))
      assign(sym, get(sym, envir, inherits=TRUE),
             pos=exportenv, inherits=FALSE)
    }
  }
  # Add task pulling function to exportenv .getTask:
  getTask <- default_getTask
  if(exists('getTask',envir=.doRedisGlobals))
    getTask <- get('getTask',envir=.doRedisGlobals)
  if(!is.null(obj$options$redis$getTask))
    getTask <- obj$options$redis$getTask
  assign(".getTask",getTask, envir=exportenv)
  
  # Define task labeling function taskLabel:
  taskLabel <- I
  if(exists('taskLabel',envir=.doRedisGlobals))
    taskLabel <- get('taskLabel',envir=.doRedisGlobals)
  if(!is.null(obj$options$redis$taskLabel))
    taskLabel <- obj$options$redis$taskLabel
  
  # Create a job environment in Redis for the workers to use
  redisSet(queueEnv, list(expr=expr, 
                          exportenv=exportenv, packages=obj$packages))
  
  # Queue the task as to be compatible with redisWorker
  # 1. Add each task block to the <queue>:<task id> hash
  # 2. Add a job ID notice to the job queue for each task block
  #
  # To speed this up, we use nonblocking calls to Redis. We also submit all
  # the tasks in a single transaction.
  redisSetPipeline(TRUE)
  redisMulti()
  taskblock <- NA
  names(taskblock) <- 1
  # Note, we're free to identify the task in any unique way.  For example, we
  # could add a data location hint.
  task_id = as.character(taskLabel(1))
  task <- list(task_id=task_id, args=taskblock)
  redisHSet(queueTasks, task_id, task)
  redisRPush(queue, ID)
  redisExec()
  redisGetResponse(all=TRUE)
  redisSetPipeline(FALSE)
  
  res <- list(ID=ID,queue=queue,status="submitted")
  class(res)<-c("doRedis_job",class(res))
  res
}
