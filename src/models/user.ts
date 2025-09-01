export interface User {
  id: string;
  email: string;
  passwordHash: string;
  roles: string[];
  allowedBusinessAreas?: string[];
  allowedChannels?: string[];
  allowedBrands?: string[];
  allowedCustomers?: string[];
  createdAt: string;
  updatedAt: string;
}


