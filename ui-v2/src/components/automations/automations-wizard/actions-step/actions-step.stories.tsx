import type { Meta, StoryObj } from "@storybook/react";

import { createFakeAutomation } from "@/mocks";
import { reactQueryDecorator } from "@/storybook/utils";
import { fn } from "@storybook/test";
import { buildApiUrl } from "@tests/utils/handlers";
import { http, HttpResponse } from "msw";
import { ActionsStep } from "./actions-step";

const MOCK_AUTOMATIONS_DATA = Array.from({ length: 5 }, createFakeAutomation);

const meta = {
	title: "Components/Automations/Wizard/ActionsStep",
	component: ActionsStep,
	args: { onPrevious: fn(), onNext: fn() },
	decorators: [reactQueryDecorator],
	parameters: {
		msw: {
			handlers: [
				http.post(buildApiUrl("/automations/filter"), () => {
					return HttpResponse.json(MOCK_AUTOMATIONS_DATA);
				}),
			],
		},
	},
} satisfies Meta;

export default meta;

export const story: StoryObj = { name: "ActionsStep" };
