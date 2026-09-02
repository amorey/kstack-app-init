import path from 'path';

import { includeIgnoreFile } from '@eslint/compat';
import js from '@eslint/js';
import { configs, plugins } from 'eslint-config-airbnb-extended';
import { rules as prettierConfigRules } from 'eslint-config-prettier';
import prettierPlugin from 'eslint-plugin-prettier';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';

const gitignorePath = path.resolve(import.meta.dirname, '.gitignore');

const jsConfig = [
  {
    name: 'js/config',
    ...js.configs.recommended,
  },
  plugins.stylistic,
  plugins.importX,
  ...configs.base.recommended,
];

// The React compiler's lint rules ship inside eslint-plugin-react-hooks, and diagnose the same
// things the Vite/babel transform (`vite.config.ts`) bails out on. Applied here rather than left to
// airbnb's react-hooks block, which enables most of them but not all — and states no intent to.
// `recommended-latest` is the set matching the installed plugin; both use the compiler's defaults,
// as does the build.
const reactConfig = [
  plugins.react,
  plugins.reactHooks,
  plugins.reactA11y,
  ...configs.react.recommended,
  reactHooks.configs.flat['recommended-latest'],
];

const typescriptConfig = [
  plugins.typescriptEslint,
  ...configs.base.typescript,
  ...configs.react.typescript,
  {
    name: 'typescript/project-service',
    languageOptions: {
      parserOptions: {
        projectService: true,
      },
    },
  },
];

const prettierConfig = [
  {
    name: 'prettier/plugin/config',
    plugins: {
      prettier: prettierPlugin,
    },
  },
  {
    name: 'prettier/config',
    rules: {
      ...prettierConfigRules,
      'prettier/prettier': 'error',
    },
  },
];

const reactRefreshConfig = [reactRefresh.configs.recommended, reactRefresh.configs.vite];

const customRulesConfig = [
  {
    name: 'custom/rules',
    rules: {
      '@typescript-eslint/consistent-type-definitions': 'off',
      'consistent-return': 'off',
      'import-x/extensions': 'off',
      'import-x/no-extraneous-dependencies': 'off',
      'import-x/no-rename-default': 'off',
      'import-x/no-unresolved': 'off',
      'import-x/prefer-default-export': 'off',
      'max-classes-per-file': 'off',
      'no-console': 'off',
      'no-underscore-dangle': 'off',
      'prettier/prettier': ['error', { singleQuote: true, printWidth: 120 }],
      'react/function-component-definition': 'off',
      'react/jsx-no-target-blank': 'off',
      'react/prop-types': 'off',
      'react/react-in-jsx-scope': 'off',
      'react/require-default-props': 'off',
      // On, and an error: the compiler is a speed-up, not the thing keeping a dep array
      // honest. Code here is written to be correct without it.
      'react-hooks/exhaustive-deps': 'error',
      'react-refresh/only-export-components': 'off',
      '@typescript-eslint/no-use-before-define': 'off',
    },
  },
];

// Everything the webview displays is text chosen by whoever controls the cluster, so an HTML sink
// in `src/` hands a hostile cluster script execution in a page that holds the whole cluster
// surface. Scoped rather than folded into `custom/rules`, which has no `files`.
const htmlSinksConfig = [
  {
    name: 'custom/no-html-sinks',
    files: ['src/**/*.{ts,tsx}'],
    rules: {
      // airbnb ships this as a warning, and a warning does not fail `pnpm lint`.
      'react/no-danger': 'error',
      'no-restricted-syntax': [
        'error',
        // airbnb's four, restated: flat config replaces a rule's options rather than merging
        // them, so omitting these here would silently turn them off.
        { selector: 'ForInStatement', message: 'Use Object.{keys,values,entries} and iterate.' },
        { selector: 'ForOfStatement', message: 'Use array methods rather than for-of.' },
        { selector: 'LabeledStatement', message: 'Labels obscure control flow.' },
        { selector: 'WithStatement', message: '`with` is disallowed in strict mode.' },
        // The DOM's HTML sinks, by dot and by bracket.
        {
          selector:
            'MemberExpression[property.name=/^(innerHTML|outerHTML|insertAdjacentHTML|createContextualFragment|write|writeln)$/]',
          message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
        },
        {
          selector:
            'MemberExpression[computed=true][property.value=/^(innerHTML|outerHTML|insertAdjacentHTML|createContextualFragment|write|writeln)$/]',
          message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
        },
        // React spells the attribute `srcDoc`; the DOM spells it `srcdoc`.
        { selector: 'JSXAttribute[name.name=/^srcdoc$/i]', message: 'No inline frames from data.' },
        // Belt and braces: the CSP already refuses these at runtime.
        { selector: "CallExpression[callee.name='eval']", message: 'No eval in the webview.' },
        { selector: "NewExpression[callee.name='Function']", message: 'No eval in the webview.' },
      ],
    },
  },
];

export default [
  includeIgnoreFile(gitignorePath),
  { ignores: ['**/dist', 'src/gql/**'] },
  ...jsConfig,
  ...reactConfig,
  ...typescriptConfig,
  ...prettierConfig,
  ...reactRefreshConfig,
  ...customRulesConfig,
  ...htmlSinksConfig,
];
