# Risuko 贡献指南

开始贡献之前，确保你已经理解了 [GitHub 的协作流程](https://guides.github.com/introduction/flow/)。

## 🌍 翻译指南

Risuko 使用 [i18next](https://www.i18next.com/overview/getting-started) 作为翻译支持库，所以你可能需要简单了解一下它的使用方法。

配置文件按照语言 (**locale**) 划分目录：`src/shared/locales`，如：`src/shared/locales/en-US` 和 `src/shared/locales/zh-CN`。

每个语言目录下按功能模块划分 TypeScript 文件：

- about.ts
- app.ts
- cloudSinks.ts
- edit.ts
- health.ts
- help.ts
- index.ts
- menu.ts
- preferences.ts
- rss.ts
- share.ts
- subnav.ts
- sync.ts
- task.ts
- window.ts

菜单的国际化已经打散到了以上文件里。