const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');

function requireR2Env() {
  const accountId = process.env.R2_ACCOUNT_ID;
  const accessKeyId = process.env.R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
  const bucketName = process.env.R2_BUCKET_NAME;

  if (!accountId || !accessKeyId || !secretAccessKey || !bucketName) {
    throw new Error('R2 não configurado.');
  }
  return { accountId, accessKeyId, secretAccessKey, bucketName };
}

function isR2Configured() {
  return Boolean(
    process.env.R2_ACCOUNT_ID &&
    process.env.R2_ACCESS_KEY_ID &&
    process.env.R2_SECRET_ACCESS_KEY &&
    process.env.R2_BUCKET_NAME
  );
}

function getClient() {
  const { accountId, accessKeyId, secretAccessKey } = requireR2Env();
  return new S3Client({
    region: 'auto',
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId, secretAccessKey },
  });
}

async function putPrivateObject(key, data, contentType = 'application/pdf') {
  const { bucketName } = requireR2Env();
  const client = getClient();
  await client.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    Body: data,
    ContentType: contentType,
    CacheControl: 'private, no-store',
  }));
  return { key };
}

async function getPrivateObjectBuffer(key) {
  const { bucketName } = requireR2Env();
  const client = getClient();
  const response = await client.send(new GetObjectCommand({
    Bucket: bucketName,
    Key: key,
  }));
  if (!response.Body) throw new Error('Arquivo não encontrado no R2.');
  const chunks = [];
  for await (const chunk of response.Body) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function deletePrivateObject(key) {
  const { bucketName } = requireR2Env();
  const client = getClient();
  await client.send(new DeleteObjectCommand({
    Bucket: bucketName,
    Key: key,
  }));
}

module.exports = {
  isR2Configured,
  putPrivateObject,
  getPrivateObjectBuffer,
  deletePrivateObject,
};
