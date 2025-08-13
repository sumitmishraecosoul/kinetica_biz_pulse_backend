import { Router } from 'express';
import { dashboardController } from '@/controllers/dashboardController';

const router = Router();

/**
 * @route GET /api/v1/dashboard/overview
 * @desc Get dashboard overview with aggregated data, business areas, and channels
 * @access Public
 */
router.get('/overview', dashboardController.getDashboardOverview);

/**
 * @route GET /api/v1/dashboard/business-areas
 * @desc Get business areas performance data
 * @access Public
 */
router.get('/business-areas', dashboardController.getBusinessAreas);

/**
 * @route GET /api/v1/dashboard/channels
 * @desc Get channels performance data
 * @access Public
 */
router.get('/channels', dashboardController.getChannels);

/**
 * @route GET /api/v1/dashboard/performance-data
 * @desc Get detailed performance data with filtering and pagination
 * @access Public
 */
router.get('/performance-data', dashboardController.getPerformanceData);

/**
 * @route GET /api/v1/dashboard/filter-options
 * @desc Get available filter options for all dimensions
 * @access Public
 */
router.get('/filter-options', dashboardController.getFilterOptions);

/**
 * @route GET /api/v1/dashboard/data-health
 * @desc Get data health check and summary information
 * @access Public
 */
router.get('/data-health', dashboardController.getDataHealth);

export default router;





