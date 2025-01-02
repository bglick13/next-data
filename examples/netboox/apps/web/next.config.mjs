/** @type {import('next').NextConfig} */
const nextConfig = {
  transpilePackages: ["@workspace/ui"],
  experimental: {
    dynamicIO: true,
    ppr: true,
    inlineCss: true,
  },
}

export default nextConfig
