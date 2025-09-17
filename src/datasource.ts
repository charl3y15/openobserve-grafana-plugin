import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  QueryFixAction,
  DataSourceWithSupplementaryQueriesSupport,
  SupplementaryQueryType,
  LogLevel,
} from '@grafana/data';
import { Observable } from 'rxjs';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { queryLogsVolume } from './features/log/LogsModel';

import { MyQuery, MyDataSourceOptions, CachedQuery } from './types';
import { logsErrorMessage, getConsumableTime } from 'utils/zincutils';
import { getOrganizations } from 'services/organizations';
import { cloneDeep } from 'lodash';
import { getGraphDataFrame, getLogsDataFrame } from 'features/log/queryResponseBuilder';
import { buildQuery } from './features/query/queryBuilder';

const REF_ID_STARTER_LOG_VOLUME = 'log-volume-';

export class DataSource
  extends DataSourceApi<MyQuery, MyDataSourceOptions>
  implements DataSourceWithSupplementaryQueriesSupport<MyQuery>
{
  instanceSettings?: DataSourceInstanceSettings<MyDataSourceOptions>;
  url: string;
  streamFields: any[];
  cachedLogsQuery: CachedQuery;
  cachedHistogramQuery: CachedQuery;
  timestampColumn: string;
  histogramQuery: any;

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.url || '';
    this.instanceSettings = instanceSettings;
    this.streamFields = [];
    this.cachedLogsQuery = {
      requestQuery: '',
      isFetching: false,
      data: null,
      promise: null,
    };
    this.cachedHistogramQuery = {
      requestQuery: '',
      isFetching: false,
      data: null,
      promise: null,
    };
    this.timestampColumn = instanceSettings.jsonData.timestamp_column;
    this.histogramQuery = null;
  }

  applyTemplateVariables(query: MyQuery, scopedVars: any): MyQuery {
    return {
      ...query,
      query: getTemplateSrv().replace(query.query || '', scopedVars),
    };
  }

  async query(options: DataQueryRequest<MyQuery>): Promise<DataQueryResponse> {
    const timestamps = getConsumableTime(options.range);
    // console.log('timestamps', timestamps);
    // console.log('options', options);
    const interpolatedTargets = options.targets.map((target) => {
      return this.applyTemplateVariables(target, options.scopedVars);
    });

    // console.log("options", options);

    const promises = interpolatedTargets.map((target) => {
      const isHistogramQuery = target?.refId?.includes(REF_ID_STARTER_LOG_VOLUME);
      let currentCache = isHistogramQuery ? this.cachedHistogramQuery : this.cachedLogsQuery;

      let reqData = buildQuery(target, timestamps, this.streamFields, options.app, this.timestampColumn);

      // // If its a logs search query, we need to save the histogram query from the request
      // // We store it in the histogramQuery variable and use it later for the graph query if the request is for a graph
      if (!target.refId?.includes(REF_ID_STARTER_LOG_VOLUME)) {
        this.histogramQuery = reqData;
      } else if (target.refId?.includes(REF_ID_STARTER_LOG_VOLUME) && this.histogramQuery) {
        reqData = this.histogramQuery;
        reqData.query.sql_mode = 'context';
        delete reqData.query.size;
      }

      const cacheKey = JSON.stringify({
        reqData,
        displayMode: target.displayMode ?? 'auto',
        type: target.refId,
      });

      console.log('cacheKey', cacheKey);
      console.log('cached request query', currentCache);
      console.log('Is same', currentCache.requestQuery === cacheKey);
      if (cacheKey === currentCache.requestQuery && currentCache.data) {
        return currentCache.data
          ?.then((res) => {
            console.log('res in cache', target, res);
            const mode = target.displayMode || 'auto';
            // console.log('mode', mode);
            // console.log('res in cache', res);
            if (target?.refId?.includes(REF_ID_STARTER_LOG_VOLUME)) {
              return res;
            }
            if (options.app === 'panel-editor' || options.app === 'dashboard') {
              if (mode === 'graph' || mode === 'auto') {
                return res;
              }
            }
            return res;
          });
      } else {
        if(target?.refId?.includes(REF_ID_STARTER_LOG_VOLUME)){
          this.resetHistogramQueryCache();
        } else {
          this.resetLogsQueryCache();
        }

        currentCache = isHistogramQuery ? this.cachedHistogramQuery : this.cachedLogsQuery;

        currentCache.data = new Promise((resolve, reject) => {
          currentCache.promise = {
            resolve,
            reject,
          };
        });
      }

      currentCache.requestQuery = cacheKey;
      currentCache.isFetching = true;
      return this.doRequest(target, reqData)
        .then((response) => {
          if (options.app === 'panel-editor' || options.app === 'dashboard') {
            const mode = target.displayMode || 'auto';
            if(mode === 'graph'){
              const graphDf = getGraphDataFrame(response.hits, target, options.app, this.timestampColumn);
              currentCache.promise?.resolve(graphDf);
              return graphDf;
            } else {
              const logsDf = getLogsDataFrame(response.hits, target, this.streamFields, this.timestampColumn);
              currentCache.promise?.resolve(logsDf);
              return logsDf;
            }
          }

          // Handle histogram queries for log volume using partitions
          if (target?.refId?.includes(REF_ID_STARTER_LOG_VOLUME) && this.histogramQuery) {
            // First, get partition information for histogram queries
            return this.doPartitionRequest(target, this.histogramQuery.query)
              .then((partitionResponse) => {
                // Check if partitions are available
                if (partitionResponse?.partitions?.length > 0) {
                  // Use partitions to make histogram requests
                  const partitions = partitionResponse.partitions;

                  if (!partitionResponse.is_histogram_eligible) {
                    let dataFrame = getGraphDataFrame([], target, options.app, this.timestampColumn);
                    currentCache.promise?.resolve(dataFrame);
                    return dataFrame;
                  }

                  const histogramPromises = partitions.map((partition: any) => {
                    // Create histogram query for each partition
                    const partitionHistogramQuery = {
                      ...this.histogramQuery,
                      query: {
                        ...this.histogramQuery.query,
                        start_time: partition[0],
                        end_time: partition[1],
                        histogram_interval: partitionResponse.histogram_interval,
                      },
                    };

                    return this.doHistogramRequest(target, partitionHistogramQuery);
                  });

                  // Combine results from all partitions
                  return Promise.all(histogramPromises).then((histogramResponses) => {
                    // Merge histogram data from all partitions
                    const combinedHits = histogramResponses.reduce((acc, response) => {
                      return acc.concat(response.hits || []);
                    }, []);

                    const graphDataFrame = getGraphDataFrame(combinedHits, target, options.app, 'zo_sql_key');
                    currentCache.promise?.resolve(graphDataFrame);

                    return graphDataFrame;
                  });
                } else {
                  // Fallback to direct histogram request if no partitions
                  return this.doHistogramRequest(target, this.histogramQuery).then((histogramResponse) => {
                    const graphDataFrame = getGraphDataFrame(
                      histogramResponse.hits,
                      target,
                      options.app,
                      this.timestampColumn
                    );
                    currentCache.promise?.resolve(graphDataFrame);
                    return graphDataFrame;
                  });
                }
              })
              .catch((error) => {
                console.error('Partition or histogram request failed:', error);
                // Fallback to empty graph
                const graphDataFrame = getGraphDataFrame([], target, options.app, this.timestampColumn);
                currentCache.promise?.resolve(graphDataFrame);
                return graphDataFrame;
              });
          } else if (target?.refId?.includes(REF_ID_STARTER_LOG_VOLUME)) {
            const graphDataFrame = getGraphDataFrame([], target, options.app, this.timestampColumn);
            currentCache.promise?.resolve(graphDataFrame);
            return graphDataFrame;
          } else {
            const logsDataFrame = getLogsDataFrame(response.hits, target, this.streamFields, this.timestampColumn);
            currentCache.promise?.resolve(logsDataFrame);
            return logsDataFrame;
          }
        })
        .catch((err) => {
          currentCache.promise?.reject(err);
          let error = {
            message: '',
            detail: '',
          };
          if (err.data) {
            error.message = err.data?.message;
            error.detail = err.data?.error_detail;
          } else {
            error.message = err.statusText;
          }

          const customMessage = logsErrorMessage(err.data.code);
          if (customMessage) {
            error.message = customMessage;
          }
          throw new Error(error.message + (error.detail ? ` ( ${error.detail} ) ` : ''));
        })
        .finally(() => {
          currentCache.isFetching = false;
        });
    });

    return Promise.all(promises).then((data) => {
      return { data: data || [] };
    });
  }

  doRequest(target: any, data: any) {
    const searchType = 'ui';
    const useCache = true;
    const pageType = 'logs';

    const url =
      this.url + `/api/${target.organization}/_search?type=${pageType}&search_type=${searchType}&use_cache=${useCache}`;

    return getBackendSrv().post(url, data, {
      showErrorAlert: false,
    });
  }

  doPartitionRequest(target: any, data: any) {
    const pageType = 'logs';
    const enableAlignHistogram = true;

    const url =
      this.url +
      `/api/${target.organization}/_search_partition?type=${pageType}&enable_align_histogram=${enableAlignHistogram}`;

    return getBackendSrv().post(url, data, {
      showErrorAlert: false,
    });
  }

  doHistogramRequest(target: any, data: any) {
    const searchType = 'ui';
    const useCache = true;
    const pageType = 'logs';

    const url =
      this.url +
      `/api/${target.organization}/_search?type=${pageType}&search_type=${searchType}&use_cache=${useCache}&is_ui_histogram=true`;

    return getBackendSrv().post(url, data, {
      showErrorAlert: false,
    });
  }

  resetHistogramQueryCache() {
    this.cachedHistogramQuery = {
      requestQuery: '',
      isFetching: false,
      data: null,
      promise: null,
    };
  }

  resetLogsQueryCache() {
    this.cachedLogsQuery = {
      requestQuery: '',
      isFetching: false,
      data: null,
      promise: null,
    };
  }

  async testDatasource() {
    return getOrganizations({ url: this.url })
      .then((res) => {
        return {
          status: 'success',
          message: 'Data source successfully connected.',
        };
      })
      .catch((error) => {
        const info: string = error?.data?.message ?? '';
        const infoInParentheses = info !== '' ? ` (${info})` : '';
        return {
          status: 'error',
          message: `Unable to connect ZincObserve ${infoInParentheses}. Verify that ZincObserve is correctly configured`,
        };
      });
  }

  modifyQuery(query: MyQuery, action: QueryFixAction): any {
    if (!action.options) {
      return query;
    }

    let expression = query.query ?? '';
    switch (action.type) {
      case 'ADD_FILTER': {
        if (expression.length > 0) {
          expression += ' and ';
        }
        expression += `${action.options.key}='${action.options.value}'`;
        break;
      }
      case 'ADD_FILTER_OUT': {
        if (expression.length > 0) {
          expression += ' and ';
        }
        expression += `${action.options.key}!='${action.options.value}'`;
        break;
      }
    }
    return { ...query, query: expression };
  }

  updateStreamFields(streamFields: any[]) {
    this.streamFields = [...streamFields];
  }

  getDataProvider(
    type: SupplementaryQueryType,
    request: DataQueryRequest<MyQuery>
  ): Observable<DataQueryResponse> | undefined {
    if (!this.getSupportedSupplementaryQueryTypes().includes(type)) {
      return undefined;
    }
    // console.log('type', type);
    // console.log('request', request);

    switch (type) {
      case SupplementaryQueryType.LogsVolume:
        return this.getLogsVolumeDataProvider(request);
      default:
        return undefined;
    }
  }

  getSupportedSupplementaryQueryTypes(): SupplementaryQueryType[] {
    return [SupplementaryQueryType.LogsVolume];
    // return [SupplementaryQueryType.LogsVolume, SupplementaryQueryType.LogsSample];
  }

  getSupplementaryQuery(type: SupplementaryQueryType, query: MyQuery): MyQuery | undefined {
    return undefined;
  }

  getLogsVolumeDataProvider(request: DataQueryRequest<MyQuery>): Observable<DataQueryResponse> | undefined {
    const logsVolumeRequest = cloneDeep(request);
    const targets = logsVolumeRequest.targets.map((target) => {
      target['refId'] = REF_ID_STARTER_LOG_VOLUME + target.refId;
      return target;
    });

    if (!targets.length) {
      return undefined;
    }

    return queryLogsVolume(
      this,
      { ...logsVolumeRequest, targets },
      {
        extractLevel: () => LogLevel.unknown,
        range: logsVolumeRequest.range,
        targets: logsVolumeRequest.targets,
      }
    );
  }
}
