const grafanaConfig = require('@grafana/eslint-config/flat');

// Note: @stylistic/eslint-plugin-ts deprecation warning is from @grafana/eslint-config
// This will be resolved when Grafana updates their config package.
// The warning does not affect functionality.

module.exports = [
  {
    ignores: [
      '.github',
      '.yarn',
      '**/build/',
      '**/compiled/',
      '**/dist/',
      'node_modules',
      '**/*.d.ts',
    ],
  },
  ...grafanaConfig,
  {
    files: ['**/*.{ts,tsx,js,jsx}'],
    rules: {
      'react/prop-types': 'off',
    },
  },
];
