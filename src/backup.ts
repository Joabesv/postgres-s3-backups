import { spawn } from "child_process";
import { createGzip } from "zlib";
import { S3Client, S3ClientConfig, PutObjectCommandInput } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

import { env } from "./env.js";

// Streams pg_dump's output straight into the S3 upload (pg_dump | gzip -> Upload),
// instead of buffering the whole dump on disk first. Writing a multi-hundred-MB
// file to a container's tmpfs/page cache counts toward billed memory usage just
// like an in-process buffer would, so this keeps memory bounded to the streaming
// chunk size regardless of database size.
const dumpAndUpload = async ({ name }: { name: string }) => {
  if (env.SUPPORT_OBJECT_LOCK) {
    throw new Error(
      "SUPPORT_OBJECT_LOCK requires hashing the full archive up front and isn't supported by the streaming backup path."
    );
  }

  const bucket = env.AWS_S3_BUCKET;
  const key = env.BUCKET_SUBFOLDER ? `${env.BUCKET_SUBFOLDER}/${name}` : name;

  const backupOptions = env.BACKUP_OPTIONS.split(/\s+/u).filter(Boolean);
  const child = spawn(
    "pg_dump",
    [`--dbname=${env.BACKUP_DATABASE_URL}`, "--format=tar", ...backupOptions],
    { stdio: ["ignore", "pipe", "pipe"] }
  );

  let stderrOutput = "";
  child.stderr.on("data", (chunk) => {
    stderrOutput += chunk.toString();
  });

  const gzip = createGzip();
  child.stdout.pipe(gzip);

  // pg_dump exiting non-zero after already streaming partial output would
  // otherwise look like a normal end-of-stream to the uploader - destroying
  // the gzip stream turns that into an upload failure instead of a silently
  // truncated backup.
  child.on("error", (error) => gzip.destroy(error));
  child.on("close", (code) => {
    if (code !== 0) {
      gzip.destroy(new Error(`pg_dump exited with code ${code}: ${stderrOutput.trimEnd()}`));
    }
  });

  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE,
  };

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);
    clientOptions.endpoint = env.AWS_S3_ENDPOINT;
  }

  const params: PutObjectCommandInput = {
    Bucket: bucket,
    Key: key,
    Body: gzip,
  };

  const client = new S3Client(clientOptions);

  console.log("Streaming DB dump to S3...");
  await new Upload({ client, params }).done();

  if (stderrOutput !== "") {
    console.log({ stderr: stderrOutput.trimEnd() });
  }

  console.log("Backup uploaded to S3...");
};

export const backup = async () => {
  console.log("Initiating DB backup...");

  const date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, "-");
  const filename = `${env.BACKUP_FILE_PREFIX}-${timestamp}.tar.gz`;

  await dumpAndUpload({ name: filename });

  console.log("DB backup complete...");
};
