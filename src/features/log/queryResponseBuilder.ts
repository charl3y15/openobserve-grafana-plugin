import { FieldType, MutableDataFrame, PreferredVisualisationType } from '@grafana/data';
import { MyQuery } from '../../types';
import { convertTimeToMs, getFieldType } from '../../utils/zincutils';

const isTimeField = (name: string, timestampColumn: string): boolean =>
  name === timestampColumn || name.startsWith('x_axis');

export const getLogsDataFrame = (
  data: any,
  target: MyQuery,
  streamFields: any = [],
  timestampColumn = '_timestamp'
) => {
  const logsData = getDefaultDataFrame(target.refId, 'logs');

  logsData.addField({
    name: 'Time',
    type: FieldType.time,
  });
  logsData.addField({
    name: 'Content',
    type: FieldType.string,
  });

  streamFields.forEach((field: any) => {
    logsData.addField({
      name: field.name,
      type: getFieldType(field.type),
    });
  });

  data.forEach((log: any) => {
    logsData.add({ ...log, Content: JSON.stringify(log), Time: convertTimeToMs(log[timestampColumn]) });
  });

  return logsData;
};

export const getGraphDataFrame = (data: any, target: MyQuery, app: string, timestampColumn = '_timestamp') => {
  const graphData = getDefaultDataFrame(target.refId, 'graph');

  let fields = ['zo_sql_key', 'zo_sql_num'];

  for (let i = 0; i < fields.length; i++) {
    if (isTimeField(fields[i], timestampColumn)) {
      graphData.addField({
        config: {
          filterable: true,
        },
        name: 'Time',
        type: FieldType.time,
      });
    } else {
      graphData.addField({
        name: fields[i],
        type: FieldType.number,
      });
    }
  }

  if (!data.length) {
    return graphData;
  }

  data.forEach((log: any) => {
    graphData.add(getField(log, fields, 'zo_sql_key'));
  });

  return graphData;
};

const getField = (log: any, columns: any, timestampColumn: string) => {
  let field: any = {};

  for (let i = 0; i < columns.length; i++) {
    let col_name = columns[i];
    let col_value = log[col_name];
    if (isTimeField(col_name, timestampColumn)) {
      // We have to convert microseconds if we receive them
      // 500 billion / year 17814 is probably a good threshold for milliseconds

      if (col_value > 500_000_000_000) {
        col_value = convertTimeToMs(col_value);
        field['Time'] = col_value;
      } else {
        // Convert any other date fmt
        field['Time'] = new Date(col_value + 'Z').getTime();
      }
    } else {
      field[col_name] = log[col_name];
    }
  }

  return field;
};

export const getDefaultDataFrame = (refId: string, visualisationType: PreferredVisualisationType = 'logs') => {
  return new MutableDataFrame({
    refId: refId,
    meta: {
      preferredVisualisationType: visualisationType,
    },
    fields: [],
  });
};
