import { MyQuery, TimeRange } from 'types';

export const buildQuery = (
  queryData: MyQuery,
  timestamps: TimeRange,
  streamFields: any[],
  app: string,
  timestampColumn: string
) => {
  try {
    let query: string = queryData.query || '';

    let req: any = {
      query: {
        sql: 'select * from "[INDEX_NAME]" [WHERE_CLAUSE]',
        start_time: timestamps.startTimeInMicro,
        end_time: timestamps.endTimeInMirco,
        size: 300,
      }
    };

    if (app !== 'explore') {
      req.query.size = 0;
    }

    if (queryData.sqlMode) {
      req.query.sql = queryData.query;
      req.query['sql_mode'] = 'full';
    }

    if (!queryData.sqlMode) {
      let whereClause = query;

      if (query.trim().length) {
        whereClause = whereClause
          .replace(/=(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' =')
          .replace(/>(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' >')
          .replace(/<(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' <');

        whereClause = whereClause
          .replace(/!=(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' !=')
          .replace(/! =(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' !=')
          .replace(/< =(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' <=')
          .replace(/> =(?=(?:[^"']*"[^"']*"')*[^"']*$)/g, ' >=');

        const parsedSQL = whereClause.split(' ');
        streamFields.forEach((field: any) => {
          parsedSQL.forEach((node: any, index: any) => {
            if (node === field.name) {
              node = node.replaceAll('"', '');
              parsedSQL[index] = '"' + node + '"';
            }
          });
        });

        whereClause = parsedSQL.join(' ');

        req.query.sql = req.query.sql.replace('[WHERE_CLAUSE]', ' WHERE ' + whereClause);
      } else {
        req.query.sql = req.query.sql.replace('[WHERE_CLAUSE]', '');
      }

      req.query.sql = req.query.sql.replace('[INDEX_NAME]', queryData.stream);
    }

    // req['encoding'] = 'base64';
    // req.query.sql = b64EncodeUnicode(req.query.sql);

    return req;
  } catch (e) {
    console.log('error in building query:', e);
  }
};
