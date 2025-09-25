import { Router } from 'express';
import Joi from 'joi';
import { dashboardController } from '@/controllers/dashboardController';
import { analyticsService } from '@/services/analyticsService';
import { logger } from '@/utils/logger';

const router = Router();

const querySchema = Joi.object({
  year: Joi.number().integer().min(2000).max(2100).optional(),
  period: Joi.string().optional(), // Allow any period from filter options
  month: Joi.string().optional(), // Allow any month name format
  businessArea: Joi.string().optional(),
  brand: Joi.string().optional(),
  category: Joi.string().optional(),
  subCategory: Joi.string().optional(),
  channel: Joi.string().optional(),
  customer: Joi.string().optional(),
  metric: Joi.string().optional(),
  limit: Joi.number().integer().min(1).max(1000).optional(),
  dimension: Joi.string().optional(),
  page: Joi.number().integer().min(1).optional(),
  pageSize: Joi.number().integer().min(1).max(200).optional(),
}).unknown(true);

function validateQuery(req: any, res: any, next: any) {
  const { error } = querySchema.validate(req.query, { abortEarly: false });
  if (error) {
    return res.status(400).json({ success: false, error: { code: 'INVALID_QUERY', message: 'Invalid query parameters', details: error.details.map((d: any) => d.message) } });
  }
  next();
}

/**
 * @route GET /api/v1/dashboard/overview
 * @desc Get dashboard overview with aggregated data, business areas, and channels
 * @access Public
 */
router.get('/overview', validateQuery, dashboardController.getDashboardOverview.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/business-areas
 * @desc Get business areas performance data
 * @access Public
 */
router.get('/business-areas', validateQuery, dashboardController.getBusinessAreas.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/channels
 * @desc Get channels performance data
 * @access Public
 */
router.get('/channels', validateQuery, dashboardController.getChannels.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/performance-data
 * @desc Get detailed performance data with filtering and pagination
 * @access Public
 */
router.get('/performance-data', validateQuery, dashboardController.getPerformanceData.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/filter-options
 * @desc Get available filter options for all dimensions
 * @access Public
 */
router.get('/filter-options', validateQuery, dashboardController.getFilterOptions.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/data-health
 * @desc Get data health check and summary information
 * @access Public
 */
router.get('/data-health', validateQuery, dashboardController.getDataHealth.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/trend
 * @desc Get trend analysis time series
 * @access Public
 */
router.get('/trend', validateQuery, dashboardController.getTrend.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/top-performers
 * @desc Get top performers
 * @access Public
 */
router.get('/top-performers', validateQuery, dashboardController.getTopPerformers.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/risk
 * @desc Get risk analysis
 * @access Public
 */
router.get('/risk', validateQuery, dashboardController.getRisk.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/variance
 * @desc Get variance analysis
 * @access Public
 */
router.get('/variance', validateQuery, dashboardController.getVariance.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/categories
 * @desc Get category performance data
 * @access Public
 */
router.get('/categories', validateQuery, dashboardController.getCategoryPerformance.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/subcategories
 * @desc Get sub-category performance data
 * @access Public
 */
router.get('/subcategories', validateQuery, dashboardController.getSubCategories.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/aggregates
 * @desc Get aggregated metrics
 * @access Public
 */
router.get('/aggregates', validateQuery, dashboardController.getAggregates.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/customers
 * @desc Get customer performance data
 * @access Public
 */
router.get('/customers', validateQuery, dashboardController.getCustomerPerformance.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/customer-overview
 * @desc Get customer overview cards data
 * @access Public
 */
router.get('/customer-overview', validateQuery, dashboardController.getCustomerOverview.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/top-customers
 * @desc Get top customers data
 * @access Public
 */
router.get('/top-customers', validateQuery, dashboardController.getTopCustomers.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/customer-channels
 * @desc Get customer channel share analysis
 * @access Public
 */
router.get('/customer-channels', validateQuery, dashboardController.getCustomerChannels.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/business-area-detailed
 * @desc Get detailed business area metrics
 * @access Public
 */
router.get('/business-area-detailed', validateQuery, dashboardController.getBusinessAreaDetailedMetrics.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/export-csv
 * @desc Export filtered data as CSV
 * @access Public
 */
router.get('/export-csv', validateQuery, dashboardController.exportCSV.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/business-area-summary
 * @desc Get reports business area summary with Excel formulas
 * @access Public
 */
router.get('/reports/business-area-summary', validateQuery, dashboardController.getReportsBusinessAreaSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/channel-summary
 * @desc Get reports channel summary with Excel formulas
 * @access Public
 */
router.get('/reports/channel-summary', validateQuery, dashboardController.getReportsChannelSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/sales-to-fgp
 * @desc Get sales to fGP summary with detailed sales breakdown data
 * @access Public
 */
router.get('/reports/sales-to-fgp', validateQuery, dashboardController.getSalesToFGPSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/trend-by-month
 * @desc Get trend by month summary with monthly performance data
 * @access Public
 */
router.get('/reports/trend-by-month', validateQuery, dashboardController.getTrendByMonthSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/customer-summary
 * @desc Get customer summary with customer-level performance data
 * @access Public
 */
router.get('/reports/customer-summary', validateQuery, dashboardController.getCustomerSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/total-brands
 * @desc Get total brands summary with brand-level performance data
 * @access Public
 */
router.get('/reports/total-brands', validateQuery, dashboardController.getTotalBrandsSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/food-brands
 * @desc Get food brands summary with brand-level performance data
 * @access Public
 */
router.get('/reports/food-brands', validateQuery, dashboardController.getFoodBrandsSummary.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/food-brands-details
 * @desc Get food brands details with sub-category level performance data
 * @access Public
 */
router.get('/reports/food-brands-details', validateQuery, dashboardController.getFoodBrandsDetails.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/household-brands
 * @desc Get household brands performance data
 * @access Public
 */
router.get('/reports/household-brands', validateQuery, dashboardController.getHouseholdBrands.bind(dashboardController));

/**
 * @route GET /api/v1/dashboard/reports/household-brands-details
 * @desc Get household brands details with sub-category level performance data
 * @access Public
 */
router.get('/reports/household-brands-details', validateQuery, dashboardController.getHouseholdBrandsDetails.bind(dashboardController));

  // Dashboard Charts endpoints
  router.post('/fgp-by-business', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getFGPByBusiness(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in fGP by Business endpoint:', error);
      res.status(500).json({ error: 'Failed to get fGP by Business data' });
    }
  });

  router.post('/fgp-by-channel', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getFGPByChannel(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in fGP by Channel endpoint:', error);
      res.status(500).json({ error: 'Failed to get fGP by Channel data' });
    }
  });

  router.post('/fgp-monthly-trend', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getFGPMonthlyTrend(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in fGP Monthly Trend endpoint:', error);
      res.status(500).json({ error: 'Failed to get fGP Monthly Trend data' });
    }
  });

  router.post('/gsales-by-business', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getGSalesByBusiness(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in gSales by Business endpoint:', error);
      res.status(500).json({ error: 'Failed to get gSales by Business data' });
    }
  });

  router.post('/gsales-by-channel', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getGSalesByChannel(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in gSales by Channel endpoint:', error);
      res.status(500).json({ error: 'Failed to get gSales by Channel data' });
    }
  });

  router.post('/gsales-monthly-trend', async (req, res) => {
    try {
      const filters = req.body;
      const data = await analyticsService.getGSalesMonthlyTrend(filters);
      res.json(data);
    } catch (error) {
      logger.error('Error in gSales Monthly Trend endpoint:', error);
      res.status(500).json({ error: 'Failed to get gSales Monthly Trend data' });
    }
  });

export default router;





