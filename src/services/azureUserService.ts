import { BlobServiceClient } from '@azure/storage-blob';
import fs from 'fs';
import path from 'path';
import { logger } from '@/utils/logger';

export class AzureUserService {
  private blobServiceClient: BlobServiceClient;
  private containerName: string;
  private blobFolder: string;
  private userFileName: string;

  constructor() {
    // Use the same Azure credentials as the main Azure service
    const connectionString = 'DefaultEndpointsProtocol=https;AccountName=kineticadbms;AccountKey=JfMzO69p3Ip+Sz+YkXxp7sHxZw0O/JunSaS5qKnSSQnxk1lPhwiQwnGyyJif7sGB01l9amAdvU/t+ASthIK/ZQ==;EndpointSuffix=core.windows.net';
    this.blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    this.containerName = 'thrive-worklytics';
    this.blobFolder = 'Biz-Pulse';
    this.userFileName = 'User_directory.csv';

    logger.info('Azure User Service initialized', {
      containerName: this.containerName,
      blobFolder: this.blobFolder,
      userFileName: this.userFileName,
      accountName: this.blobServiceClient.accountName
    });
  }

  /**
   * Upload User_directory.csv to Azure Blob Storage
   */
  async uploadUserFile(): Promise<boolean> {
    try {
      const localFilePath = path.join(process.cwd(), 'server', 'database', 'User_directory.csv');
      
      // Check if local file exists
      if (!fs.existsSync(localFilePath)) {
        logger.error('Local User_directory.csv file not found');
        return false;
      }

      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.userFileName}`;
      const blockBlobClient = containerClient.getBlockBlobClient(blobPath);

      // Read local file
      const fileContent = fs.readFileSync(localFilePath, 'utf-8');
      
      // Upload to Azure
      await blockBlobClient.upload(fileContent, Buffer.byteLength(fileContent), {
        blobHTTPHeaders: {
          blobContentType: 'text/csv'
        }
      });

      logger.info(`Successfully uploaded User_directory.csv to Azure: ${blobPath}`);
      return true;
    } catch (error) {
      logger.error('Error uploading User_directory.csv to Azure:', error);
      return false;
    }
  }

  /**
   * Download User_directory.csv from Azure Blob Storage
   */
  async downloadUserFile(): Promise<string | null> {
    try {
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.userFileName}`;
      const blobClient = containerClient.getBlobClient(blobPath);

      // Check if blob exists
      const exists = await blobClient.exists();
      if (!exists) {
        logger.warn(`User_directory.csv not found in Azure: ${blobPath}`);
        return null;
      }

      // Download blob
      const downloadResponse = await blobClient.download();
      if (!downloadResponse.readableStreamBody) {
        throw new Error('Failed to download User_directory.csv from Azure');
      }

      // Convert stream to string
      const chunks: Buffer[] = [];
      for await (const chunk of downloadResponse.readableStreamBody) {
        chunks.push(Buffer.from(chunk));
      }
      const content = Buffer.concat(chunks).toString('utf-8');

      logger.info(`Successfully downloaded User_directory.csv from Azure: ${blobPath}`);
      return content;
    } catch (error) {
      logger.error('Error downloading User_directory.csv from Azure:', error);
      return null;
    }
  }

  /**
   * List all files in the Biz-Pulse folder
   */
  async listFiles(): Promise<string[]> {
    try {
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const files: string[] = [];
      
      for await (const blob of containerClient.listBlobsFlat({ prefix: this.blobFolder })) {
        files.push(blob.name);
      }

      logger.info(`Found ${files.length} files in Azure Biz-Pulse folder`);
      return files;
    } catch (error) {
      logger.error('Error listing files in Azure:', error);
      return [];
    }
  }

  /**
   * Get the Azure URL for the User_directory.csv file
   */
  getAzureUrl(): string {
    return `https://kineticadbms.blob.core.windows.net/${this.containerName}/${this.blobFolder}/${this.userFileName}`;
  }

  /**
   * Check if User_directory.csv exists in Azure
   */
  async fileExists(): Promise<boolean> {
    try {
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.userFileName}`;
      const blobClient = containerClient.getBlobClient(blobPath);
      
      return await blobClient.exists();
    } catch (error) {
      logger.error('Error checking if User_directory.csv exists in Azure:', error);
      return false;
    }
  }

  /**
   * Get file properties (last modified, size, etc.)
   */
  async getFileProperties(): Promise<any> {
    try {
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.userFileName}`;
      const blobClient = containerClient.getBlobClient(blobPath);
      
      const properties = await blobClient.getProperties();
      return {
        lastModified: properties.lastModified,
        contentLength: properties.contentLength,
        contentType: properties.contentType,
        etag: properties.etag
      };
    } catch (error) {
      logger.error('Error getting User_directory.csv properties from Azure:', error);
      return null;
    }
  }
}

// Lazy initialization
let _azureUserService: AzureUserService | null = null;

export const getAzureUserService = (): AzureUserService => {
  if (!_azureUserService) {
    _azureUserService = new AzureUserService();
  }
  return _azureUserService;
};
