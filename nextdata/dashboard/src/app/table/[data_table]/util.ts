import { z } from "zod";
import { TableMetadata } from "./types";

export const jsTypeToSparkDtype = {
  string: "STRING",
  number: "DOUBLE",
  bigint: "LONG",
  boolean: "BOOLEAN",
  null: "STRING",
  undefined: "STRING",
  timestamp: "TIMESTAMP",
  date: "DATE",
};

export const DTYPE_TO_ZOD_TYPE = {
  string: z.string(),
  double: z.number(),
  long: z.number(),
  boolean: z.boolean(),
  timestamp: z.date(),
  date: z.date(),
};

export const tableMetadataToZodSchema = (
  metadata: TableMetadata | undefined
) => {
  if (!metadata?.schema) {
    return z.object({});
  }
  const schema = metadata.schema
    .map(([column_name, column_type]) => ({
      [column_name]:
        DTYPE_TO_ZOD_TYPE[column_type as keyof typeof DTYPE_TO_ZOD_TYPE],
    }))
    .reduce((acc, curr) => ({ ...acc, ...curr }), {});
  return z.object(schema);
};

export const transformSampleData = (
  data: string[][],
  schema: z.ZodObject<any>
) => {
  // Data comes back as an array of arrays, we need it to be an array of dicts corresponding to the schema
  const transformed = data.map((row) => {
    const transformedRow: Record<string, unknown> = {};
    row.forEach((value, index) => {
      const key = Object.keys(schema.shape)[index];
      transformedRow[key] = value;
    });
    return transformedRow;
  });
  return transformed;
};
