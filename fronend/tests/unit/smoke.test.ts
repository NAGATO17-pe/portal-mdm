import { describe, expect, it } from "vitest";
import { cn } from "@/lib/utils";

describe("utils", () => {
  it("combina clases tailwind", () => {
    expect(cn("p-2", "p-4")).toBe("p-4");
  });
});
