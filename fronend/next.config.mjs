/** @type {import('next').NextConfig} */
const allowedOrigins = (process.env.NEXT_ALLOWED_DEV_ORIGINS ?? "")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  experimental: {
    optimizePackageImports: ["lucide-react"]
  },
  allowedDevOrigins: allowedOrigins
};

export default nextConfig;
