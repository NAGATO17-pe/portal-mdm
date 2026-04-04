/** @type {import('next').NextConfig} */
<<<<<<< HEAD
const allowedOrigins = (process.env.NEXT_ALLOWED_DEV_ORIGINS ?? "")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

=======
>>>>>>> main
const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  experimental: {
    optimizePackageImports: ["lucide-react"]
<<<<<<< HEAD
  },
  allowedDevOrigins: allowedOrigins
=======
  }
>>>>>>> main
};

export default nextConfig;
