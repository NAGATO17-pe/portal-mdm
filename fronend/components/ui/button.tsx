import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const buttonVariants = cva(
<<<<<<< HEAD
  "inline-flex items-center justify-center rounded-md text-sm font-medium transition duration-300 disabled:cursor-not-allowed disabled:opacity-50",
  {
    variants: {
      variant: {
        primary: "bg-primary text-white shadow-[0_6px_24px_rgba(37,99,235,0.4)] hover:scale-[1.01] hover:opacity-95",
=======
  "inline-flex items-center justify-center rounded-md text-sm font-medium transition disabled:cursor-not-allowed disabled:opacity-50",
  {
    variants: {
      variant: {
        primary: "bg-primary text-white hover:opacity-90",
>>>>>>> main
        ghost: "bg-transparent hover:bg-white/10"
      },
      size: {
        md: "h-10 px-4",
        sm: "h-8 px-3"
      }
    },
    defaultVariants: {
      variant: "primary",
      size: "md"
    }
  }
);

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(({ className, variant, size, ...props }, ref) => {
  return <button ref={ref} className={cn(buttonVariants({ variant, size }), className)} {...props} />;
});

Button.displayName = "Button";
