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

checkFailed=function(obj){
  ## foreach object is augmented when options.redis.async=TRUE
  queue=obj$queue
  ID=obj$ID
  task_list=obj$task_list
  nout=obj$nout
  it=obj$stored_it
  accumulator=obj$accumulator
  
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
  
  started <- gsub(sprintf("%s:%.0f.start.",queue,ID),"",started)
  alive <- gsub(sprintf("%s:%.0f.alive.",queue,ID),"",alive)
  fault <- setdiff(started,alive)
  if(length(fault)>0) {
    # One or more worker faults have occurred. Re-submit the work.
    for(k in fault)
    {
      warning(sprintf("Worker fault, resubmitting task %s.",k))
      qs <- sprintf("%s:%.0f.start.%s",queue,ID,k)
      redisDelete(qs)
      queueTasks <- sprintf("%s:%.0f",queue,ID) # Job tasks hash
      redisHSet(queueTasks, k, task_list[[k]])
      redisRPush(queue, ID)
    }
  }
  obj
}