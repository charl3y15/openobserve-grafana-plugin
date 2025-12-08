import { FieldType, DataFrame, Field } from '@grafana/data';
import { MyQuery } from '../../types';
import { convertTimeToMs, getFieldType } from '../../utils/zincutils';

/**
 * Checks if a value looks like a timestamp by examining its format
 */
const isTimestampValue = (value: any): boolean => {
  if (value === null || value === undefined) {
    return false;
  }

  // Check if it's a large number (timestamp in ms or microseconds)
  // Timestamps are typically > 1 billion (after Sep 2001)
  if (typeof value === 'number' && value > 1_000_000_000) {
    return true;
  }

  // Check if it's an ISO 8601 date string (e.g., "2025-12-08T09:34:50")
  if (typeof value === 'string') {
    const isoDatePattern = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/;
    if (isoDatePattern.test(value)) {
      return true;
    }

    // Also check if it's a valid date string that can be parsed
    const dateTest = new Date(value);
    if (!isNaN(dateTest.getTime()) && value.length >= 10) {
      return true;
    }
  }

  return false;
};

/**
 * Detects which field is the timestamp by checking actual values
 */
const detectTimestampField = (data: any[]): string | null => {
  if (!data || data.length === 0) {
    return null;
  }

  const firstRow = data[0];
  const fields = Object.keys(firstRow);

  // Check each field's value to see if it looks like a timestamp
  for (const field of fields) {
    const value = firstRow[field];
    if (isTimestampValue(value)) {
      return field;
    }
  }

  return null;
};

/**
 * Gets field names from response data
 */
const getFieldsFromData = (data: any[]): string[] => {
  if (!data || data.length === 0) {
    return [];
  }
  return Object.keys(data[0]);
};

/**
 * Infers Grafana field type from a value
 */
const inferFieldType = (value: any): FieldType => {
  if (value === null || value === undefined) {
    return FieldType.string;
  }
  if (typeof value === 'number') {
    return FieldType.number;
  }
  if (typeof value === 'boolean') {
    return FieldType.boolean;
  }
  return FieldType.string;
};

/**
 * Converts various timestamp formats to milliseconds
 */
const convertToTimeMs = (value: any): number => {
  if (typeof value === 'number') {
    // Check if it's in microseconds (> 500 billion)
    if (value > 500_000_000_000) {
      return convertTimeToMs(value);
    }
    // Check if it's in seconds (< 10 billion, roughly year 2286)
    if (value < 10_000_000_000) {
      return value * 1000;
    }
    // Already in milliseconds
    return value;
  }

  // Handle ISO 8601 date strings like "2025-12-08T09:34:50"
  if (typeof value === 'string') {
    // Add 'Z' if no timezone info to treat as UTC
    const dateString = value.includes('Z') || value.includes('+') || value.match(/-\d{2}:\d{2}$/)
      ? value
      : value + 'Z';
    return new Date(dateString).getTime();
  }

  return new Date(value).getTime();
};

export const getLogsDataFrame = (
  data: any,
  target: MyQuery,
  streamFields: any = [],
  timestampColumn = '_timestamp'
): DataFrame => {
  // Build fields array
  const fields: Field[] = [
    {
      name: 'Time',
      type: FieldType.time,
      config: {},
      values: [],
    },
    {
      name: 'Content',
      type: FieldType.string,
      config: {},
      values: [],
    },
  ];

  // Add stream fields
  streamFields.forEach((field: any) => {
    fields.push({
      name: field.name,
      type: getFieldType(field.type),
      config: {},
      values: [],
    });
  });

  // Populate field values
  data.forEach((log: any) => {
    fields[0].values.push(convertTimeToMs(log[timestampColumn])); // Time
    fields[1].values.push(JSON.stringify(log)); // Content

    // Add stream field values
    streamFields.forEach((field: any, index: number) => {
      fields[index + 2].values.push(log[field.name]);
    });
  });

  return {
    refId: target.refId,
    meta: {
      preferredVisualisationType: 'logs',
    },
    fields,
    length: data.length,
  };
};

export const getGraphDataFrame = (
  data: any,
  target: MyQuery,
  app: string,
  timestampColumn = '_timestamp'
): DataFrame => {
  // Get actual fields from response data instead of hardcoding
  let fieldNames = data.length > 0 ? getFieldsFromData(data) : [];

  // Detect which field is the timestamp by checking values
  const detectedTimeField = detectTimestampField(data);
  const timeFieldName = detectedTimeField || timestampColumn;

  // If no data, use default fields for empty state
  if (!fieldNames.length) {
    fieldNames = ['zo_sql_key', 'zo_sql_num', 'x_axis_1'];
  }

  // Build fields array
  const fields: Field[] = [];

  for (let i = 0; i < fieldNames.length; i++) {
    const fieldName = fieldNames[i];
    const isTime = fieldName === timeFieldName;

    if (isTime) {
      fields.push({
        name: 'Time',
        type: FieldType.time,
        config: { filterable: true },
        values: [],
      });
    } else {
      // Infer type from first row value
      const fieldType = data.length > 0 ? inferFieldType(data[0][fieldName]) : FieldType.number;
      fields.push({
        name: fieldName,
        type: fieldType,
        config: {},
        values: [],
      });
    }
  }

  // Populate field values
  data.forEach((log: any) => {
    const processedRow = getField(log, fieldNames, timeFieldName);

    fields.forEach((field) => {
      const fieldName = field.name === 'Time' ? 'Time' : field.name;
      field.values.push(processedRow[fieldName]);
    });
  });

  return {
    refId: target.refId,
    meta: {
      preferredVisualisationType: 'graph',
    },
    fields,
    length: data.length,
  };
};

const getField = (log: any, columns: any, timestampColumn: string) => {
  let field: any = {};

  for (let i = 0; i < columns.length; i++) {
    let col_name = columns[i];
    let col_value = log[col_name];

    // Check if this column is the timestamp column
    if (col_name === timestampColumn) {
      // Use the helper function to handle all timestamp formats
      field['Time'] = convertToTimeMs(col_value);
    } else {
      field[col_name] = log[col_name];
    }
  }

  return field;
};
