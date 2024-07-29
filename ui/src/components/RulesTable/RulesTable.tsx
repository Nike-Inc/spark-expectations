import React, { useState, useMemo } from 'react';
import { useTable, useBlockLayout, useResizeColumns } from 'react-table';
import { ScrollArea, Tooltip, Text, Button, Group } from '@mantine/core';
import styled from 'styled-components';
import { useMutation } from '@tanstack/react-query';
import yaml from 'js-yaml';
import { notifications } from '@mantine/notifications';
import { useRepoFile } from '@/api';
import { useRepoStore } from '@/store';
import { Loading } from '@/components';
import { RulesModal } from '../RulesModal';
import { commitChangesFn } from '@/api/commits/github/commit-flie-mutation';

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

    .th.edit,
    .td.edit {
      position: sticky;
      right: 0;
      background-color: inherit; /* Ensure the background matches the row */
      z-index: 1; /* Make sure it's above other content */
    }
  }
`;

// Custom YAML type to always use double quotes for strings
const customStringType = new yaml.Type('!str', {
  kind: 'scalar',
  resolve: (data) => typeof data === 'string',
  construct: (data) => data,
  instanceOf: String,
  represent: {
    default: (str) => `"${str}"`,
  },
});

// Custom schema that includes the custom string type
const customYamlSchema = yaml.DEFAULT_SCHEMA.extend([customStringType]);
const transformKey = (key: string) =>
  key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());

export const RulesTable: React.FC = () => {
  const [expandedRowIndex, setExpandedRowIndex] = useState<number | null>(null);
  const [editRowData, setEditRowData] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  const { selectedRepo, selectedFile } = useRepoStore((state) => ({
    selectedRepo: state.selectedRepo,
    selectedFile: state.selectedFile,
  }));

  const { data, isLoading, isError } = useRepoFile(
    selectedRepo?.owner?.login,
    selectedRepo?.name,
    selectedFile?.path
  );

  const commitMutation = useMutation({
    mutationFn: commitChangesFn,
  });

  const columns = useMemo(() => {
    const baseColumns = data?.rules_data
      ? Object.keys(data.rules_data[0]).map((key) => ({
          Header: transformKey(key),
          accessor: key,
          minWidth: 150, // Set a minimum width for each column
          getHeaderProps: () => ({ className: key }), // Add getHeaderProps for each column
        }))
      : [];

    if (baseColumns.length === 0) {
      return [];
    }

    // // Add edit column
    // baseColumns.push({
    //   Header: 'Edit',
    //   accessor: 'edit', // This accessor can be anything that doesn't conflict with your data keys
    //   minWidth: 100,
    //   Cell: ({ row }) => <Button onClick={() => handleEdit(row.original)}>Edit</Button>,
    //   getHeaderProps: () => ({ className: 'edit' }), // Add getHeaderProps for the edit column
    // });

    return baseColumns;
  }, [data]);

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

  const handleEdit = (row: object | null, rowIndex: number) => {
    // @ts-ignore
    setEditRowData({ ...row, index: rowIndex });
    setIsModalOpen(true);
  };

  const handleSave = (updatedRowData: { index: any }) => {
    try {
      const updatedTableData = tableData.map((row: any, index: any) =>
        index === updatedRowData.index ? updatedRowData : row
      );
      const yamlData = yaml.dump({ rules_data: updatedTableData }, { schema: customYamlSchema });
      commitMutation.mutateAsync({ content: yamlData });
    } catch (err) {
      notifications.show({
        title: 'Error',
        message: 'Error saving changes',
        color: 'danger',
      });
    }
  };

  if (rows && rows.length === 0) {
    return null;
  }

  return (
    <ScrollArea>
      <Styles>
        <div {...getTableProps()} className="table">
          <div>
            {headerGroups.map((headerGroup) => (
              <div {...headerGroup.getHeaderGroupProps()} className="tr">
                {headerGroup.headers.map((column) => {
                  const headerProps = column.getHeaderProps();
                  return (
                    <div {...headerProps} className={`th ${headerProps.className}`}>
                      {column.render('Header')}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>

          <div {...getTableBodyProps()}>
            {rows.map((row, index) => {
              prepareRow(row);
              const isExpanded = expandedRowIndex === index;
              return (
                <div key={row.id}>
                  <Group
                    {...row.getRowProps()}
                    className="tr"
                    onClick={() => handleRowClick(index)}
                    unstyled
                  >
                    {row.cells.map((cell) => (
                      <Tooltip key={cell.column.id} label={String(cell.value)} withArrow>
                        <div
                          {...cell.getCellProps({
                            className: cell.column.id === 'edit' ? 'td edit' : 'td',
                          })}
                        >
                          {cell.render('Cell')}
                        </div>
                      </Tooltip>
                    ))}
                  </Group>
                  {isExpanded && (
                    <div className="expandedRowContent">
                      {Object.entries(row.original).map(([key, value]) => (
                        <div key={key}>
                          <strong>{transformKey(key)}:</strong> {String(value)}
                        </div>
                      ))}
                      <Button w="100px" onClick={() => handleEdit(row.original, index)}>
                        Edit
                      </Button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </Styles>
      {isModalOpen && (
        <RulesModal
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
          rowData={editRowData}
          onSave={handleSave}
        />
      )}
    </ScrollArea>
  );
};
