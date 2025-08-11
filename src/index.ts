// import dotenv from 'dotenv';
// import path from 'path';

// Load environment variables FIRST, before any other imports
// dotenv.config({ path: path.join(__dirname, '../.env') });

// Debug: Check if environment variables are loaded
console.log('Environment check:');
console.log('AZURE_STORAGE_CONNECTION_STRING:', 'HARDCODED FOR DEBUGGING');
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('PORT:', process.env.PORT);

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import compression from 'compression';
import rateLimit from 'express-rate-limit';

// Import middleware
import { requestLogger, errorLogger } from '@/utils/logger';

// Import routes
import dashboardRoutes from '@/routes/dashboard';

// Import services
import { cacheService } from '@/services/cacheService';
import { getAzureService } from '@/services/azureService';

const app = express();
const PORT = process.env.PORT || 5000;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com"],
      scriptSrc: ["'self'"],
      imgSrc: ["'self'", "data:", "https:"],
      fontSrc: ["'self'", "https://fonts.gstatic.com"],
      connectSrc: ["'self'", "https://kineticadbms.blob.core.windows.net"]
    }
  }
}));

// CORS configuration
app.use(cors({
  origin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
}));

// Rate limiting
const limiter = rateLimit({
  windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '900000'), // 15 minutes
  max: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '100'), // limit each IP to 100 requests per windowMs
  message: {
    success: false,
    error: {
      code: 'RATE_LIMIT_EXCEEDED',
      message: 'Too many requests from this IP, please try again later.'
    }
  },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use('/api/', limiter);

// Compression middleware
app.use(compression());

// Body parsing middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Logging middleware
app.use(morgan('combined'));
app.use(requestLogger);

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    message: 'Kinetica Biz-Pulse API is running',
    timestamp: new Date().toISOString(),
    environment: NODE_ENV,
    version: process.env.npm_package_version || '1.0.0'
  });
});

// API routes
app.use('/api/v1/dashboard', dashboardRoutes);

// Root endpoint
app.get('/api/v1', (req, res) => {
  res.json({
    success: true,
    message: 'Kinetica Biz-Pulse API',
    version: '1.0.0',
    endpoints: {
      dashboard: '/api/v1/dashboard',
      customers: '/api/v1/customers',
      brands: '/api/v1/brands',
      categories: '/api/v1/categories',
      analytics: '/api/v1/analytics'
    }
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: {
      code: 'NOT_FOUND',
      message: `Route ${req.originalUrl} not found`
    }
  });
});

// Error handling middleware
app.use(errorLogger);

app.use((error: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Unhandled error:', error);
  
  res.status(error.status || 500).json({
    success: false,
    error: {
      code: error.code || 'INTERNAL_SERVER_ERROR',
      message: error.message || 'An unexpected error occurred',
      ...(NODE_ENV === 'development' && { stack: error.stack })
    }
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  await cacheService.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  await cacheService.disconnect();
  process.exit(0);
});

// Start server
const startServer = async () => {
  try {
    // Test Azure connection
    console.log('Testing Azure connection...');
    const azureService = getAzureService();
    
    // First test basic connection
    const connectionTest = await azureService.testConnection();
    if (!connectionTest) {
      throw new Error('Failed to connect to Azure Blob Storage');
    }
    
    // Then try to get data summary
    await azureService.getDataSummary();
    console.log('✅ Azure connection successful');

    // Test cache connection
    console.log('Testing cache connection...');
    await cacheService.set('test', 'test', 60);
    console.log('✅ Cache connection successful');

    app.listen(PORT, () => {
      console.log(`🚀 Kinetica Biz-Pulse API server running on port ${PORT}`);
      console.log(`📊 Environment: ${NODE_ENV}`);
      console.log(`🔗 Health check: http://localhost:${PORT}/health`);
      console.log(`📚 API docs: http://localhost:${PORT}/api/v1`);
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
};

startServer();

export default app;
