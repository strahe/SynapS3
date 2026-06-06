import { defineConfig, type DefaultTheme, type HeadConfig } from 'vitepress'

const defaultSiteUrl = 'https://synaps3.strahe.com'

function normalizeBase(value: string | undefined) {
  const raw = value?.trim()
  if (!raw) {
    return '/'
  }

  const normalized = raw.replace(/^\/+|\/+$/g, '')
  return normalized ? `/${normalized}/` : '/'
}

function normalizeSiteUrl(value: string | undefined) {
  return (value?.trim() || defaultSiteUrl).replace(/\/+$/, '')
}

const base = normalizeBase(process.env.VITEPRESS_BASE)
const siteUrl = normalizeSiteUrl(process.env.VITEPRESS_SITE_URL)
const siteBaseUrl = `${siteUrl}${base}`
const gtagId = process.env.VITEPRESS_GTAG_ID?.trim()

const head: HeadConfig[] = [
  ['meta', { name: 'theme-color', content: '#0f766e' }],
  ['link', { rel: 'icon', href: `${base}favicon.svg` }],
]

if (gtagId && /^[A-Za-z0-9-]+$/.test(gtagId)) {
  const encodedGtagId = JSON.stringify(gtagId)

  head.push(
    [
      'script',
      {
        async: '',
        src: `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(gtagId)}`,
      },
    ],
    [
      'script',
      {},
      `window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}
gtag('js', new Date());
gtag('config', ${encodedGtagId});`,
    ],
  )
}

const enNav: DefaultTheme.NavItem[] = [
  { text: 'Get Started', link: '/en/getting-started/overview' },
  { text: 'Deploy', link: '/en/getting-started/docker' },
  { text: 'Operate', link: '/en/operations/production-checklist' },
  { text: 'Reference', link: '/en/reference/s3-compatibility' },
]

const zhNav: DefaultTheme.NavItem[] = [
  { text: '入门', link: '/zh/getting-started/overview' },
  { text: '部署', link: '/zh/getting-started/docker' },
  { text: '运维', link: '/zh/operations/production-checklist' },
  { text: '参考', link: '/zh/reference/s3-compatibility' },
]

const enSidebar: DefaultTheme.Sidebar = [
  {
    text: 'Getting Started',
    items: [
      { text: 'Overview', link: '/en/getting-started/overview' },
      { text: 'Quick Start', link: '/en/getting-started/quick-start' },
      { text: 'S3 Clients', link: '/en/getting-started/s3-clients' },
    ],
  },
  {
    text: 'Deployment & Configuration',
    items: [
      { text: 'Docker Deployment', link: '/en/getting-started/docker' },
      { text: 'Build from Source', link: '/en/getting-started/source' },
      { text: 'Configuration Model', link: '/en/configuration/model' },
      { text: 'Environment Variables', link: '/en/configuration/environment' },
      { text: 'Runtime Data', link: '/en/configuration/runtime-data' },
    ],
  },
  {
    text: 'Operations',
    items: [
      { text: 'Production Checklist', link: '/en/operations/production-checklist' },
      { text: 'Health and Metrics', link: '/en/operations/health-metrics' },
      { text: 'Upgrade and Recovery', link: '/en/operations/upgrade-recovery' },
      { text: 'Troubleshooting', link: '/en/operations/troubleshooting' },
    ],
  },
  {
    text: 'Concepts',
    items: [
      { text: 'Architecture', link: '/en/concepts/architecture' },
      { text: 'Write Path and Cache', link: '/en/concepts/write-path-cache' },
      { text: 'Filecoin Storage Flow', link: '/en/concepts/filecoin-storage-flow' },
    ],
  },
  {
    text: 'Reference',
    items: [
      { text: 'S3 Compatibility', link: '/en/reference/s3-compatibility' },
      { text: 'CLI Reference', link: '/en/reference/cli-api' },
      { text: 'Admin API', link: '/en/reference/admin-api' },
    ],
  },
]

const zhSidebar: DefaultTheme.Sidebar = [
  {
    text: '入门',
    items: [
      { text: '概览', link: '/zh/getting-started/overview' },
      { text: '快速开始', link: '/zh/getting-started/quick-start' },
      { text: 'S3 客户端', link: '/zh/getting-started/s3-clients' },
    ],
  },
  {
    text: '部署与配置',
    items: [
      { text: 'Docker 部署', link: '/zh/getting-started/docker' },
      { text: '源码构建', link: '/zh/getting-started/source' },
      { text: '配置模型', link: '/zh/configuration/model' },
      { text: '环境变量', link: '/zh/configuration/environment' },
      { text: '运行数据', link: '/zh/configuration/runtime-data' },
    ],
  },
  {
    text: '运维',
    items: [
      { text: '生产环境检查清单', link: '/zh/operations/production-checklist' },
      { text: '健康检查与指标', link: '/zh/operations/health-metrics' },
      { text: '升级与恢复', link: '/zh/operations/upgrade-recovery' },
      { text: '故障排查', link: '/zh/operations/troubleshooting' },
    ],
  },
  {
    text: '概念',
    items: [
      { text: '架构', link: '/zh/concepts/architecture' },
      { text: '写入路径与缓存', link: '/zh/concepts/write-path-cache' },
      { text: 'Filecoin 存储流程', link: '/zh/concepts/filecoin-storage-flow' },
    ],
  },
  {
    text: '参考',
    items: [
      { text: 'S3 兼容性', link: '/zh/reference/s3-compatibility' },
      { text: 'CLI 参考', link: '/zh/reference/cli-api' },
      { text: 'Admin API', link: '/zh/reference/admin-api' },
    ],
  },
]

type MarkdownFenceRenderer = (
  tokens: Array<{ content: string; info: string; map: [number, number] | null }>,
  idx: number,
  options: unknown,
  env: { path?: string; relativePath?: string } | undefined,
  self: { renderToken: MarkdownFenceRenderer },
) => string

type MarkdownItLike = {
  renderer: {
    rules: {
      fence?: MarkdownFenceRenderer
    }
  }
}

function stableHash(value: string) {
  let hash = 0

  for (let index = 0; index < value.length; index += 1) {
    hash = Math.imul(31, hash) + value.charCodeAt(index)
  }

  return (hash >>> 0).toString(36)
}

function configureMermaidMarkdown(md: MarkdownItLike) {
  const defaultFence = md.renderer.rules.fence

  md.renderer.rules.fence = (tokens, idx, options, env, self) => {
    const token = tokens[idx]
    const language = token.info.trim().split(/\s+/, 1)[0]

    if (language !== 'mermaid' && language !== 'mmd') {
      return defaultFence
        ? defaultFence(tokens, idx, options, env, self)
        : self.renderToken(tokens, idx, options, env, self)
    }

    const source = `${env?.path || env?.relativePath || 'page'}:${idx}:${token.map?.join('-') || ''}`
    const id = `mermaid-${stableHash(source)}`
    const code = encodeURIComponent(token.content)

    return `<ClientOnly><MermaidBlock id="${id}" code="${code}" /></ClientOnly>`
  }
}

export default defineConfig({
  title: 'SynapS3',
  titleTemplate: ':title | SynapS3',
  description: 'An S3-compatible gateway for Filecoin storage.',
  base,
  lang: 'en-US',
  lastUpdated: true,
  ignoreDeadLinks: false,
  sitemap: {
    hostname: siteBaseUrl,
  },
  head,
  markdown: {
    config: configureMermaidMarkdown,
  },
  transformPageData(pageData) {
    if (pageData.relativePath !== 'index.md') {
      return
    }

    pageData.frontmatter.head ??= []
    pageData.frontmatter.head.push([
      'link',
      {
        rel: 'canonical',
        href: `${siteBaseUrl}en/`,
      },
    ])
  },
  themeConfig: {
    logo: '/favicon.svg',
    externalLinkIcon: true,
    search: {
      provider: 'local',
    },
    editLink: {
      pattern: 'https://github.com/strahe/SynapS3/edit/main/docs/:path',
      text: 'Edit this page on GitHub',
    },
    footer: {
      message: 'An S3-compatible gateway for Filecoin storage.',
      copyright: 'Copyright © 2026-present SynapS3 contributors',
    },
    socialLinks: [
      { icon: 'github', link: 'https://github.com/strahe/SynapS3' },
    ],
  },
  locales: {
    en: {
      label: 'English',
      lang: 'en-US',
      title: 'SynapS3',
      description: 'An S3-compatible gateway for Filecoin storage.',
      themeConfig: {
        nav: enNav,
        sidebar: enSidebar,
        outline: {
          level: [2, 3],
          label: 'On this page',
        },
        docFooter: {
          prev: 'Previous',
          next: 'Next',
        },
      },
    },
    zh: {
      label: '简体中文',
      lang: 'zh-CN',
      title: 'SynapS3',
      description: '基于 Filecoin 的 S3 兼容网关。',
      themeConfig: {
        nav: zhNav,
        sidebar: zhSidebar,
        outline: {
          level: [2, 3],
          label: '本页目录',
        },
        docFooter: {
          prev: '上一页',
          next: '下一页',
        },
        editLink: {
          pattern: 'https://github.com/strahe/SynapS3/edit/main/docs/:path',
          text: '在 GitHub 上编辑此页',
        },
        footer: {
          message: '基于 Filecoin 存储的 S3 兼容网关。',
          copyright: 'Copyright © 2026-present SynapS3 贡献者',
        },
      },
    },
  },
})
