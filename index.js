'use strict'
const { DataApiClient } = require('rqlite-js')
const log = require('./logger')

const CACHE_HOSTS = ['http://bot-cache-0.bot-cache-internal.datastore.svc.cluster.local:4001', 'http://bot-cache-1.bot-cache-internal.datastore.svc.cluster.local:4001', 'http://bot-cache-2.bot-cache-internal.datastore.svc.cluster.local:4001']

module.exports = class RqliteCache {
  constructor({ rqliteHost, tableName, createTable, jsonOnly }){
    this._table_name = tableName
    this._rqlite_host = rqliteHost || CACHE_HOSTS
    this._json_only = jsonOnly
    this._client_ready = false

    this._dataApiClient = new DataApiClient(this._rqlite_host)
    if(createTable){
      this._createTable()
    }else{
      this._client_ready = true
    }
  }
  async _createTable(){
    try{
      let sql = `CREATE TABLE IF NOT EXISTS ${this._table_name} (id TEXT PRIMARY KEY, data TEXT NOT NULL, ttl TEXT NOT NULL)`
      let dataResults = await this._dataApiClient.execute(sql)
      if(dataResults?.hasError()){
        log.error(dataResults?.getFirstError(), this._table_name)
        setTimeout(()=>this._createTable(), 5000)
        return
      }
      this._client_ready = true
      log.info(`created rqlite table ${this._table_name}`, this._table_name)
    }catch(e){
      log.error(e, this._table_name)
      setTimeout(()=>this._createTable(), 5000)
    }
  }

  async all(){
    try{
      if(!this._client_ready || !this._table_name) return
      if(this._json_only) return await this.allJSON()

      let sql = `SELECT data FROM "${this._table_name}"`
      let res = await this._dataApiClient.query(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.toArray()?.filter(x=>x?.data)?.map(x=>x.data)
    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async allJSON(){
    try{
      if(!this._client_ready || !this._table_name) return

      let sql = `SELECT data FROM "${this._table_name}"`
      let res = await this._dataApiClient.query(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.toArray()?.filter(x=>x?.data)?.map(x=>JSON.parse(x.data))
    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async del(key){
    try{
      if(!key || !this._client_ready || !this._table_name) return

      let sql = `DELETE FROM "${this._table_name}" WHERE id=${key?.toString()}`
      let res = await this._dataApiClient.execute(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.get(0)?.getRowsAffected()
    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async clear(){
    try{
      if(!this._client_ready || !this._table_name) return

      let sql = `DROP TABLE "${this._table_name}"`
      let res = await this._dataApiClient.execute(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.get(0)?.getRowsAffected()
    }catch(e){
      log.error(e, this._table_name)
    }
  }
  async get(key){
    try{
      if(!key || !this._client_ready || !this._table_name) return
      if(this._json_only) return await this.getJSON(key)

      let sql = `SELECT data FROM "${this._table_name}" WHERE id="${key.toString()}"`
      let res = await this._dataApiClient.query(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      let result = res?.get(0)
      return result?.data?.data

    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async getJSON(key){
    try{
      if(!key || !this._client_ready || !this._table_name) return

      let sql = `SELECT data FROM "${this._table_name}" WHERE id="${key.toString()}"`
      let res = await this._dataApiClient.query(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      let result = res?.get(0)
      if(result?.data?.data) return JSON.parse(result?.data?.data)

    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async set(key, value){
    try{
      if(!key || !this._client_ready || !this._table_name) return
      if(this._json_only) return await this.setJSON(key, value)

      let sql = [ [ `INSERT INTO "${this._table_name}" (id, data, ttl) VALUES(:id, :data, ${Date.now()}) ON CONFLICT(id) DO UPDATE set data=:data, ttl=${Date.now()}`, { id: key.toString(), data: value?.toString() } ] ]
      let res = await this._dataApiClient.execute(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.get(0)?.getRowsAffected()
    }catch(e){
      log.error(e, this._table_name)
    }
  }

  async setJSON(key, value){
    try{
      if(!key || !value || !this._client_ready || !this._table_name) return

      let sql = [ [ `INSERT INTO "${this._table_name}" (id, data, ttl) VALUES(:id, :data, ${Date.now()}) ON CONFLICT(id) DO UPDATE set data=:data, ttl=${Date.now()}`, { id: key.toString(), data: JSON.stringify(value) } ] ]
      let res = await this._dataApiClient.execute(sql)
      if(res?.hasError()){
        log.error(res?.getFirstError(), this._table_name)
        return
      }
      return res?.get(0)?.getRowsAffected()
    }catch(e){
      log.error(e, this._table_name)
    }
  }
  status(){
    return this._client_ready
  }
}
