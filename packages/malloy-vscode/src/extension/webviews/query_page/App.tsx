/*
 * Copyright 2021 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

import { Result } from "@malloydata/malloy";
import { HTMLView } from "@malloydata/render";
import React, { useEffect, useState } from "react";
import styled from "styled-components";
import {
  QueryMessageType,
  QueryPanelMessage,
  QueryRunStatus,
} from "../../webview_message_manager";
import { Spinner } from "../components";
import { ResultKind, ResultKindToggle } from "./ResultKindToggle";
import Prism from "prismjs";
import "prismjs/components/prism-json";
import "prismjs/components/prism-sql";

enum Status {
  Ready = "ready",
  Compiling = "compiling",
  Running = "running",
  Error = "error",
  Displaying = "displaying",
  Rendering = "rendering",
  Done = "done",
}

export const App: React.FC = () => {
  const [status, setStatus] = useState<Status>(Status.Ready);
  const [html, setHTML] = useState("");
  const [json, setJSON] = useState("");
  const [sql, setSQL] = useState("");
  const [error, setError] = useState<string | undefined>(undefined);
  const [resultKind, setResultKind] = useState<ResultKind>(ResultKind.HTML);

  useEffect(() => {
    const listener = (event: MessageEvent<QueryPanelMessage>) => {
      const message = event.data;

      switch (message.type) {
        case QueryMessageType.QueryStatus:
          if (message.status === QueryRunStatus.Error) {
            setStatus(Status.Error);
            setError(message.error);
          } else {
            setError(undefined);
          }
          if (message.status === QueryRunStatus.Done) {
            setStatus(Status.Rendering);
            setTimeout(async () => {
              const result = Result.fromJSON(message.result);
              const data = result.data;
              setJSON(
                Prism.highlight(
                  JSON.stringify(data.toObject(), null, 2),
                  Prism.languages["json"],
                  "sql"
                )
              );
              setSQL(
                Prism.highlight(result.sql, Prism.languages["sql"], "sql")
              );
              const rendered = await new HTMLView().render(
                data,
                message.styles
              );
              setStatus(Status.Displaying);
              setTimeout(() => {
                setHTML(rendered);
                setStatus(Status.Done);
              }, 0);
            }, 0);
          } else {
            setHTML("");
            setJSON("");
            setSQL("");
            switch (message.status) {
              case QueryRunStatus.Compiling:
                setStatus(Status.Compiling);
                break;
              case QueryRunStatus.Running:
                setStatus(Status.Running);
                break;
            }
          }
      }
    };
    window.addEventListener("message", listener);
    return () => window.removeEventListener("message", listener);
  });

  return (
    <div
      style={{
        height: "100%",
        margin: "0",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {[
        Status.Compiling,
        Status.Running,
        Status.Rendering,
        Status.Displaying,
      ].includes(status) ? (
        <Spinner text={getStatusLabel(status) || ""} />
      ) : (
        ""
      )}
      {!error && <ResultKindToggle kind={resultKind} setKind={setResultKind} />}
      {!error && resultKind === ResultKind.HTML && (
        <Scroll>
          <div
            dangerouslySetInnerHTML={{ __html: html }}
            style={{ margin: "10px" }}
          />
        </Scroll>
      )}
      {!error && resultKind === ResultKind.JSON && (
        <Scroll>
          <PrismContainer style={{ margin: "10px" }}>
            <div
              dangerouslySetInnerHTML={{ __html: json }}
              style={{ margin: "10px" }}
            />
          </PrismContainer>
        </Scroll>
      )}
      {!error && resultKind === ResultKind.SQL && (
        <Scroll>
          <PrismContainer style={{ margin: "10px" }}>
            <div
              dangerouslySetInnerHTML={{ __html: sql }}
              style={{ margin: "10px" }}
            />
          </PrismContainer>
        </Scroll>
      )}
      {error && <div>{error}</div>}
    </div>
  );
};

function getStatusLabel(status: Status) {
  switch (status) {
    case Status.Compiling:
      return "Compiling";
    case Status.Running:
      return "Running";
    case Status.Rendering:
      return "Rendering";
    case Status.Displaying:
      return "Displaying";
  }
}

const Scroll = styled.div`
  height: 100%;
  overflow: auto;
`;

const PrismContainer = styled.pre`
  font-family: source-code-pro, Menlo, Monaco, Consolas, "Courier New",
    monospace;
  font-size: 14px;
  color: #333388;

  span.token.keyword {
    color: #af00db;
  }

  span.token.comment {
    color: #4f984f;
  }

  span.token.function,
  span.token.function_keyword {
    color: #795e26;
  }

  span.token.string {
    color: #ca4c4c;
  }

  span.token.regular_expression {
    color: #88194d;
  }

  span.token.operator,
  span.token.punctuation {
    color: #505050;
  }

  span.token.number {
    color: #09866a;
  }

  span.token.type,
  span.token.timeframe {
    color: #0070c1;
  }

  span.token.date {
    color: #09866a;
    /* color: #8730b3; */
  }

  span.token.property {
    color: #b98f13;
  }
`;
