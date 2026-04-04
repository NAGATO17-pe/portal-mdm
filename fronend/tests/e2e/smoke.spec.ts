import { test, expect } from "@playwright/test";

test("sin sesión redirige a login", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/login/);
});

test("permite login mock y acceso a dashboard", async ({ page }) => {
  await page.goto("/login");
  await page.getByLabel("Usuario").fill("demo");
  await page.getByLabel("Contraseña").fill("demo");
  await page.getByRole("button", { name: "Entrar" }).click();

  await expect(page).toHaveURL(/dashboard/);
  await expect(page.getByRole("heading", { name: "Dashboard operativo" })).toBeVisible();
});
