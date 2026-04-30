'use strict'
function getTimeStamp(timestamp){
  if(!timestamp) timestamp = Date.now()
  let dateTime = new Date(timestamp)
  return dateTime.toLocaleString('en-US', { timeZone: 'Etc/GMT+5', hour12: false })
}
module.exports.error = (err, table_name)=>{
  console.error(`${getTimeStamp(Date.now())} ERROR [${table_name}] ${err}`)
}
module.exports.info = (msg, table_name)=>{
  console.log(`${getTimeStamp(Date.now())} INFO [${table_name}] ${msg}`)
}
