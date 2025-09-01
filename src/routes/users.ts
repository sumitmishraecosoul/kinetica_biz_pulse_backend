import { Router } from 'express';
import { csvUserService } from '@/services/csvUserService';
import { getAzureUserService } from '@/services/azureUserService';

const router = Router();

/**
 * @route GET /api/v1/users
 * @desc Get all users (for admin purposes)
 * @access Public (for development)
 */
router.get('/', async (req, res) => {
  try {
    const users = await csvUserService.getAllUsers();
    
    // Remove password hashes for security
    const safeUsers = users.map(user => ({
      id: user.id,
      email: user.email,
      roles: user.roles,
      allowedBusinessAreas: user.allowedBusinessAreas,
      allowedChannels: user.allowedChannels,
      allowedBrands: user.allowedBrands,
      allowedCustomers: user.allowedCustomers,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt
    }));

    res.json({
      success: true,
      data: {
        users: safeUsers,
        totalUsers: safeUsers.length
      }
    });
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({
      success: false,
      error: {
        code: 'FETCH_USERS_ERROR',
        message: 'Failed to fetch users'
      }
    });
  }
});

/**
 * @route GET /api/v1/users/azure-status
 * @desc Get Azure file status and information
 * @access Public (for development)
 */
router.get('/azure-status', async (req, res) => {
  try {
    const azureUserService = getAzureUserService();
    
    const [fileExists, fileProperties, allFiles] = await Promise.all([
      azureUserService.fileExists(),
      azureUserService.getFileProperties(),
      azureUserService.listFiles()
    ]);

    res.json({
      success: true,
      data: {
        azureUrl: azureUserService.getAzureUrl(),
        fileExists,
        fileProperties,
        allFilesInBizPulse: allFiles,
        localFilePath: 'server/server/database/User_directory.csv'
      }
    });
  } catch (error) {
    console.error('Error fetching Azure status:', error);
    res.status(500).json({
      success: false,
      error: {
        code: 'AZURE_STATUS_ERROR',
        message: 'Failed to fetch Azure status'
      }
    });
  }
});

/**
 * @route POST /api/v1/users/upload-to-azure
 * @desc Manually upload User_directory.csv to Azure
 * @access Public (for development)
 */
router.post('/upload-to-azure', async (req, res) => {
  try {
    const azureUserService = getAzureUserService();
    const success = await azureUserService.uploadUserFile();
    
    if (success) {
      res.json({
        success: true,
        message: 'User directory uploaded to Azure successfully',
        azureUrl: azureUserService.getAzureUrl()
      });
    } else {
      res.status(500).json({
        success: false,
        error: {
          code: 'UPLOAD_FAILED',
          message: 'Failed to upload user directory to Azure'
        }
      });
    }
  } catch (error) {
    console.error('Error uploading to Azure:', error);
    res.status(500).json({
      success: false,
      error: {
        code: 'UPLOAD_ERROR',
        message: 'Failed to upload to Azure'
      }
    });
  }
});

/**
 * @route GET /api/v1/users/download-from-azure
 * @desc Download User_directory.csv from Azure
 * @access Public (for development)
 */
router.get('/download-from-azure', async (req, res) => {
  try {
    const azureUserService = getAzureUserService();
    const content = await azureUserService.downloadUserFile();
    
    if (content) {
      res.json({
        success: true,
        data: {
          content,
          azureUrl: azureUserService.getAzureUrl()
        }
      });
    } else {
      res.status(404).json({
        success: false,
        error: {
          code: 'FILE_NOT_FOUND',
          message: 'User directory file not found in Azure'
        }
      });
    }
  } catch (error) {
    console.error('Error downloading from Azure:', error);
    res.status(500).json({
      success: false,
      error: {
        code: 'DOWNLOAD_ERROR',
        message: 'Failed to download from Azure'
      }
    });
  }
});

export default router;
