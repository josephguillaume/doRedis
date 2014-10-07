#        __      ____           ___     
#   ____/ /___  / __ \___  ____/ (_)____
#  / __  / __ \/ /_/ / _ \/ __  / / ___/
# / /_/ / /_/ / _, _/  __/ /_/ / (__  ) 
# \__,_/\____/_/ |_|\___/\__,_/_/____/  
#                                      
# Copyright (c) 2010 by Bryan W. Lewis.
# Changes for asynchronous retrieval of jobs Copyright (c) 2014 by Joseph Guillaume
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

retrieve <- function(obj,...) UseMethod("retrieve")

retrieve.default <- function(obj,ID,queue,timeout=0)
  retrieve.doRedis_job(list(ID=ID,queue=queue),timeout=timeout)

print.doRedis_job <- function(x,..){
  cat(sprintf("job to be run by a redisWorker
queue: %s
ID: %s
status: %s
",x$queue,x$ID,x$status))
}

## TODO: allow value to be kept on redis server  
retrieve.doRedis_job <- function(obj,timeout=0){
  ID=obj$ID
  queue=obj$queue
  
  queueEnv <- sprintf("%s:%.0f.env",queue,ID) # R job environment
  if(!redisExists(queueEnv)) 
    stop(sprintf("Job with ID %s does not exist in queue %s. Has it already been retrieved?",ID,queue))
  
  queueResults <- sprintf("%s:%.0f.results",queue,ID) # Output values
  if(timeout==0){
    results <- redisRPop(queueResults)
  } else {
    results <- tryCatch(redisBRPop(queueResults, timeout=timeout),error=NULL)
  }
  
  if(is.null(results))
  {
    # Check for worker fault and re-submit tasks if required...
    queueStart <- sprintf("%s:%.0f.start*",queue,ID)
    queueAlive <- sprintf("%s:%.0f.alive*",queue,ID)
    redisMulti()
    redisKeys(queueStart)
    redisKeys(queueAlive)
    redisLLen(queue)    # number of queued tasks remaining
    ans <- redisExec()
    started <- ans[[1]]
    alive <- ans[[2]]
    queued <- ans[[3]]
    
    if(length(started)>0) obj$status="started"
    
    started <- gsub(sprintf("%s:%.0f.start.",queue,ID),"",started)
    alive <- gsub(sprintf("%s:%.0f.alive.",queue,ID),"",alive)
    fault <- setdiff(started,alive)
    if(length(fault)>0) {
      # Worker fault has occurred. Re-submit the work.
      warning("Worker fault, resubmitting task.")
      qs <- sprintf("%s:%.0f.start.%s",queue,ID,k)
      redisDelete(qs)
      queueTasks <- sprintf("%s:%.0f",queue,ID) # Job tasks hash
      redisHSet(queueTasks, 1, 1)
      redisRPush(queue, ID)
      obj$status <- "resubmitted after fault"
    }
    if(!inherits(obj,"doRedis_job")) class(obj)<-c("doRedis_job",class(obj))
    return(obj)
  }
  else
  {
    # Clean up the session ID and session environment
    removeJob(queue, ID)
    
    return(results[[1]])
  }
}