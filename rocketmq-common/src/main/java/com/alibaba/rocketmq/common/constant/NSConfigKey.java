/**
 *
 */
package com.alibaba.rocketmq.common.constant;

import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;

/**
 * @author holly, Lien
 * name server configuration keys
 */
public enum NSConfigKey {
  //数据中心分发比例 1:0.4,2:0.3,3:0.1,5:0.2
  DC_DISPATCH_RATIO ("DC_SELECTOR","DC_DISPATCH_RATIO"),
  //数据中心分发策略： BY_RATIO, BY_LOCATION
  DC_DISPATCH_STRATEGY("DC_SELECTOR","DC_DISPATCH_STRATEGY"),
  //数据中心分发速度策略比例，同机房    0.8
  DC_DISPATCH_STRATEGY_LOCATION_RATIO("DC_SELECTOR","DC_DISPATCH_STRATEGY_LOCATION_RATIO"),

  DC_SUSPEND_CONSUMER_BY_IP_RANGE("DC_SELECTOR", "SUSPEND_CONSUMER_BY_IP_RANGE"),


  //TRACE LEVEL: DEBUG, MEDIUM, PRODUCTION, NONE
  STALKER_LEVEL("STALKER", "LEVEL"),

  // IP range enabled.
  STALKER_IP_RANGE("STALKER", "IP_RANGE"),

  /*MQ Filter*/
  PG_MQ_FILETER_BY_DC (NamesrvUtil.NAMESPACE_PRODUCER_GROUP_FILTER_CONFIG,"DATA_CENTER_FILTER"),
  PG_MQ_FILETER_BY_BROKER (NamesrvUtil.NAMESPACE_PRODUCER_GROUP_FILTER_CONFIG,"BROKER_FILTER");


  private String namespace;
  private String key;

  private NSConfigKey(String namespace,String key){
    this.namespace = namespace;
    this.key = key;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getKey() {
    return key;
  }

}
