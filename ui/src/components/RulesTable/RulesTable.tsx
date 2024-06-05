import React, { useState, useMemo } from 'react';
import { useTable, useBlockLayout, useResizeColumns } from 'react-table';
import { Badge, ScrollArea, Tooltip, Text } from '@mantine/core';
import styled from 'styled-components';
import { useRepoFile } from '@/api';
import { useRepoStore } from '@/store';
import { Loading } from '@/components';

const Styles = styled.div`
  .table {
    display: inline-block;
    border-spacing: 0;
    border: 1px solid black;

    .tr {
      display: flex;
      flex-direction: row;
      :last-child {
        .td {
          border-bottom: 0;
        }
      }
      &:hover {
        background-color: #f1f1f1;
      }
    }

    .th,
    .td {
      flex: 1;
      margin: 0;
      padding: 0.5rem;
      border-bottom: 1px solid black;
      border-right: 1px solid black;
      position: relative;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;

      :last-child {
        border-right: 0;
      }

      .resizer {
        display: inline-block;
        background: #000;
        width: 5px;
        height: 100%;
        position: absolute;
        right: 0;
        top: 0;
        transform: translateX(50%);
        z-index: 1;
        touch-action: none;
        cursor: col-resize;
      }
    }

    .th {
      background: #f1f1f1;
      position: sticky;
      top: 0;
      z-index: 1;
    }

    .expandedRowContent {
      display: flex;
      flex-direction: column;
      padding: 10px;
      border-top: 1px solid black;
      background: #f9f9f9;
    }

    .expandedRowContent div {
      display: flex;
      flex-direction: row;
      margin-bottom: 5px;
    }

    .expandedRowContent div strong {
      min-width: 150px;
      font-weight: bold;
    }
  }
`;

const transformKey = (key: string) =>
  key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());

const renderCellContent = (key: string, value: any) => {
  if (key === 'enabled') {
    return value ? <Badge color="green">Enabled</Badge> : <Badge color="red">Disabled</Badge>;
  }
  return value;
};

export const RulesTable: React.FC = () => {
  const [expandedRowIndex, setExpandedRowIndex] = useState<number | null>(null);

  const { selectedRepo, selectedFile } = useRepoStore((state) => ({
    selectedRepo: state.selectedRepo,
    selectedFile: state.selectedFile,
  }));

  const { data, isLoading, isError } = useRepoFile(
    selectedRepo?.owner?.login,
    selectedRepo?.name,
    selectedFile?.path
  );

  const columns = useMemo(
    () =>
      data?.rules_data
        ? Object.keys(data.rules_data[0]).map((key) => ({
            Header: transformKey(key),
            accessor: key,
            minWidth: 150, // Set a minimum width for each column
          }))
        : [],
    [data]
  );

  const tableData = useMemo(() => (data ? data.rules_data : []), [data]);

  const { getTableProps, getTableBodyProps, headerGroups, rows, prepareRow } = useTable(
    {
      columns,
      data: tableData,
    },
    useBlockLayout,
    useResizeColumns
  );

  if (isLoading) {
    return <Loading />;
  }

  if (isError) {
    return <Text>Error loading data</Text>;
  }

  const handleRowClick = (index: number) => {
    setExpandedRowIndex(expandedRowIndex === index ? null : index);
  };

  return (
    <ScrollArea>
      <Styles>
        <div {...getTableProps()} className="table">
          <div>
            {headerGroups.map((headerGroup) => (
              <div {...headerGroup.getHeaderGroupProps()} className="tr">
                {headerGroup.headers.map((column) => (
                  <div {...column.getHeaderProps()} className="th">
                    {column.render('Header')}
                    <div {...column.getResizerProps()} className="resizer" />
                  </div>
                ))}
              </div>
            ))}
          </div>

          <div {...getTableBodyProps()}>
            {rows.map((row, index) => {
              prepareRow(row);
              const isExpanded = expandedRowIndex === index;
              return (
                <div key={row.id}>
                  <div {...row.getRowProps()} className="tr" onClick={() => handleRowClick(index)}>
                    {row.cells.map((cell) => (
                      <Tooltip key={cell.column.id} label={String(cell.value)} withArrow>
                        <div {...cell.getCellProps()} className="td">
                          {renderCellContent(cell.column.id, cell.value)}
                        </div>
                      </Tooltip>
                    ))}
                  </div>
                  {isExpanded && (
                    <div className="expandedRowContent">
                      {Object.entries(row.original).map(([key, value]) => (
                        <div key={key}>
                          <strong>{transformKey(key)}:</strong> {String(value)}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </Styles>
    </ScrollArea>
  );
};
