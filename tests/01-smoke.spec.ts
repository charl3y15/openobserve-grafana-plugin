import { test, expect } from '@grafana/plugin-e2e';

test('Smoke test', async ({ gotoHomePage, page }) => {
  await gotoHomePage();
  await expect(page.getByText('Welcome to Grafana')).toBeVisible();
});
